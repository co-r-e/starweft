use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use hmac::{Hmac, Mac};
use rand_core::{OsRng, RngCore};
use reqwest::Method;
use sha2::{Digest, Sha256};
use starweft_id::{ActorId, NodeId};
use starweft_p2p::{RuntimeTopology, RuntimeTransport};
use starweft_store::Store;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::cli::RegistryServeArgs;
use crate::config::{Config, DataPaths};
use crate::helpers::{
    BootstrapPeerParams, local_advertised_capabilities, local_listen_addresses,
    local_stop_public_key_helper, upsert_bootstrap_peer,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RegistryPeerRecord {
    pub(crate) actor_id: String,
    pub(crate) node_id: String,
    pub(crate) public_key: String,
    pub(crate) stop_public_key: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) listen_addresses: Vec<String>,
    pub(crate) role: String,
    pub(crate) published_at: OffsetDateTime,
}

pub(crate) type RegistryHmacSha256 = Hmac<Sha256>;

pub(crate) type HttpResponse = (&'static str, Vec<(&'static str, String)>, Vec<u8>);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryAuthHeaders {
    pub(crate) timestamp: String,
    pub(crate) nonce: String,
    pub(crate) content_sha256: String,
    pub(crate) signature: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryValidatedAuth {
    pub(crate) nonce: String,
    pub(crate) issued_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryRateLimitBucket {
    pub(crate) window_started_at: OffsetDateTime,
    pub(crate) request_count: u64,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub(crate) const REGISTRY_AUTH_SCHEME: &str = "starweft-hmac-sha256-v1";
pub(crate) const REGISTRY_AUTH_HEADER: &str = "x-starweft-registry-auth";
pub(crate) const REGISTRY_TIMESTAMP_HEADER: &str = "x-starweft-registry-timestamp";
pub(crate) const REGISTRY_NONCE_HEADER: &str = "x-starweft-registry-nonce";
pub(crate) const REGISTRY_CONTENT_SHA256_HEADER: &str = "x-starweft-registry-content-sha256";
pub(crate) const REGISTRY_SIGNATURE_HEADER: &str = "x-starweft-registry-signature";
pub(crate) const REGISTRY_AUTH_MAX_SKEW_SEC: i64 = 300;

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

pub(crate) fn resolve_registry_shared_secret(config: &Config) -> Result<Option<String>> {
    resolve_optional_secret(
        config.discovery.registry_shared_secret.as_deref(),
        config.discovery.registry_shared_secret_env.as_deref(),
        "discovery.registry_shared_secret_env",
    )
}

pub(crate) fn resolve_registry_serve_shared_secret(
    args: &RegistryServeArgs,
) -> Result<Option<String>> {
    resolve_optional_secret(
        args.shared_secret.as_deref(),
        args.shared_secret_env.as_deref(),
        "--shared-secret-env",
    )
}

pub(crate) fn resolve_optional_secret(
    inline_secret: Option<&str>,
    env_name: Option<&str>,
    env_label: &str,
) -> Result<Option<String>> {
    if let Some(secret) = inline_secret {
        if secret.is_empty() {
            bail!("[E_REGISTRY_AUTH] shared secret must not be empty");
        }
        return Ok(Some(secret.to_owned()));
    }

    let Some(env_name) = env_name else {
        return Ok(None);
    };
    match std::env::var(env_name) {
        Ok(secret) if !secret.is_empty() => Ok(Some(secret)),
        Ok(_) => bail!("[E_REGISTRY_AUTH] {env_label} points to an empty env var: {env_name}"),
        Err(std::env::VarError::NotPresent) => {
            bail!("[E_REGISTRY_AUTH] {env_label} points to a missing env var: {env_name}")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("[E_REGISTRY_AUTH] {env_label} points to a non-unicode env var: {env_name}")
        }
    }
}

pub(crate) fn registry_request_path(url: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(url)?;
    let mut path = parsed.path().to_owned();
    if let Some(query) = parsed.query() {
        path.push('?');
        path.push_str(query);
    }
    Ok(path)
}

pub(crate) fn registry_body_sha256(body: &[u8]) -> String {
    BASE64_STANDARD.encode(Sha256::digest(body))
}

pub(crate) fn generate_registry_nonce() -> String {
    let mut bytes = [0_u8; 16];
    OsRng.fill_bytes(&mut bytes);
    BASE64_STANDARD.encode(bytes)
}

pub(crate) fn registry_signature_input(
    method: &str,
    path: &str,
    timestamp: &str,
    nonce: &str,
    body_hash: &str,
) -> String {
    format!(
        "{REGISTRY_AUTH_SCHEME}\n{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        path,
        timestamp,
        nonce,
        body_hash
    )
}

pub(crate) fn build_registry_auth_headers(
    secret: &str,
    method: &str,
    path: &str,
    body: &[u8],
    now: OffsetDateTime,
) -> Result<RegistryAuthHeaders> {
    let timestamp = now.format(&Rfc3339)?;
    let nonce = generate_registry_nonce();
    let content_sha256 = registry_body_sha256(body);
    let signature =
        sign_registry_request(secret, method, path, &timestamp, &nonce, &content_sha256)?;
    Ok(RegistryAuthHeaders {
        timestamp,
        nonce,
        content_sha256,
        signature,
    })
}

pub(crate) fn sign_registry_request(
    secret: &str,
    method: &str,
    path: &str,
    timestamp: &str,
    nonce: &str,
    content_sha256: &str,
) -> Result<String> {
    let mut mac = RegistryHmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| anyhow!("[E_REGISTRY_AUTH] invalid shared secret: {error}"))?;
    mac.update(registry_signature_input(method, path, timestamp, nonce, content_sha256).as_bytes());
    Ok(BASE64_STANDARD.encode(mac.finalize().into_bytes()))
}

pub(crate) fn parse_http_headers(headers: &str) -> HashMap<String, String> {
    headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect()
}

pub(crate) fn validate_registry_auth_headers(
    secret: &str,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    now: OffsetDateTime,
) -> Result<RegistryValidatedAuth> {
    let scheme = headers
        .get(REGISTRY_AUTH_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_AUTH_HEADER}"))?;
    if scheme != REGISTRY_AUTH_SCHEME {
        bail!("[E_REGISTRY_AUTH] unsupported auth scheme: {scheme}");
    }

    let timestamp = headers
        .get(REGISTRY_TIMESTAMP_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_TIMESTAMP_HEADER}"))?;
    let issued_at = OffsetDateTime::parse(timestamp, &Rfc3339).with_context(|| {
        format!("[E_REGISTRY_AUTH] invalid {REGISTRY_TIMESTAMP_HEADER}: {timestamp}")
    })?;
    let drift_sec = (now - issued_at).whole_seconds().abs();
    if drift_sec > REGISTRY_AUTH_MAX_SKEW_SEC {
        bail!(
            "[E_REGISTRY_AUTH] timestamp drift {}s exceeds allowed skew {}s",
            drift_sec,
            REGISTRY_AUTH_MAX_SKEW_SEC
        );
    }

    let nonce = headers
        .get(REGISTRY_NONCE_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_NONCE_HEADER}"))?;
    if nonce.is_empty() {
        bail!("[E_REGISTRY_AUTH] registry nonce must not be empty");
    }

    let content_sha256 = headers
        .get(REGISTRY_CONTENT_SHA256_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_CONTENT_SHA256_HEADER}"))?;
    let expected_body_hash = registry_body_sha256(body);
    if content_sha256 != &expected_body_hash {
        bail!("[E_REGISTRY_AUTH] request body hash mismatch");
    }

    let provided_signature = headers
        .get(REGISTRY_SIGNATURE_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_SIGNATURE_HEADER}"))?;
    let signature_bytes = BASE64_STANDARD
        .decode(provided_signature)
        .with_context(|| format!("[E_REGISTRY_AUTH] invalid {REGISTRY_SIGNATURE_HEADER}"))?;
    let mut mac = RegistryHmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| anyhow!("[E_REGISTRY_AUTH] invalid shared secret: {error}"))?;
    mac.update(registry_signature_input(method, path, timestamp, nonce, content_sha256).as_bytes());
    mac.verify_slice(&signature_bytes)
        .map_err(|_| anyhow!("[E_REGISTRY_AUTH] invalid registry signature"))?;
    Ok(RegistryValidatedAuth {
        nonce: nonce.clone(),
        issued_at,
    })
}

// ---------------------------------------------------------------------------
// Replay / rate-limit
// ---------------------------------------------------------------------------

pub(crate) fn remember_registry_nonce(
    replay_cache: &mut HashMap<String, OffsetDateTime>,
    nonce: &str,
    now: OffsetDateTime,
) -> Result<()> {
    let cutoff = now - TimeDuration::seconds(REGISTRY_AUTH_MAX_SKEW_SEC);
    replay_cache.retain(|_, seen_at| *seen_at >= cutoff);
    if replay_cache.contains_key(nonce) {
        bail!("[E_REGISTRY_AUTH] replayed registry nonce");
    }
    replay_cache.insert(nonce.to_owned(), now);
    Ok(())
}

pub(crate) fn registry_rate_limit_for(
    args: &RegistryServeArgs,
    method: &str,
    path: &str,
) -> Option<u64> {
    if args.rate_limit_window_sec == 0 {
        return None;
    }
    match (method, path) {
        ("POST", "/announce") if args.announce_rate_limit > 0 => Some(args.announce_rate_limit),
        ("GET", "/peers") if args.peers_rate_limit > 0 => Some(args.peers_rate_limit),
        _ => None,
    }
}

pub(crate) fn enforce_registry_rate_limit(
    rate_limits: &mut HashMap<String, RegistryRateLimitBucket>,
    key: &str,
    limit: u64,
    window_sec: u64,
    now: OffsetDateTime,
) -> Option<u64> {
    if limit == 0 || window_sec == 0 {
        return None;
    }

    let window = TimeDuration::seconds(window_sec as i64);
    rate_limits.retain(|_, bucket| now - bucket.window_started_at < window);

    let bucket = rate_limits
        .entry(key.to_owned())
        .or_insert_with(|| RegistryRateLimitBucket {
            window_started_at: now,
            request_count: 0,
        });
    if now - bucket.window_started_at >= window {
        bucket.window_started_at = now;
        bucket.request_count = 0;
    }
    if bucket.request_count >= limit {
        let retry_after =
            (window_sec as i64 - (now - bucket.window_started_at).whole_seconds()).max(1) as u64;
        return Some(retry_after);
    }

    bucket.request_count += 1;
    None
}

// ---------------------------------------------------------------------------
// Request helpers
// ---------------------------------------------------------------------------

pub(crate) fn apply_registry_auth(
    request: reqwest::blocking::RequestBuilder,
    shared_secret: Option<&str>,
    method: Method,
    url: &str,
    body: &[u8],
) -> Result<reqwest::blocking::RequestBuilder> {
    let Some(secret) = shared_secret else {
        return Ok(request);
    };
    let path = registry_request_path(url)?;
    let headers = build_registry_auth_headers(
        secret,
        method.as_str(),
        &path,
        body,
        OffsetDateTime::now_utc(),
    )?;
    Ok(request
        .header(REGISTRY_AUTH_HEADER, REGISTRY_AUTH_SCHEME)
        .header(REGISTRY_TIMESTAMP_HEADER, headers.timestamp)
        .header(REGISTRY_NONCE_HEADER, headers.nonce)
        .header(REGISTRY_CONTENT_SHA256_HEADER, headers.content_sha256)
        .header(REGISTRY_SIGNATURE_HEADER, headers.signature))
}

pub(crate) fn next_registry_sync_at(
    config: &Config,
    now: OffsetDateTime,
) -> Option<OffsetDateTime> {
    if !config.discovery.auto_discovery || config.discovery.registry_url.is_none() {
        return None;
    }
    if config.discovery.registry_heartbeat_sec == 0 {
        return None;
    }
    Some(now + TimeDuration::seconds(config.discovery.registry_heartbeat_sec as i64))
}

pub(crate) fn registry_endpoint(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

// ---------------------------------------------------------------------------
// Registry server
// ---------------------------------------------------------------------------

pub(crate) fn run_registry_serve(args: RegistryServeArgs) -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let shared_secret = resolve_registry_serve_shared_secret(&args)?;
    let listener = TcpListener::bind(&args.bind)
        .with_context(|| format!("failed to bind registry on {}", args.bind))?;
    let entries = Arc::new(Mutex::new(HashMap::<String, RegistryPeerRecord>::new()));
    let replay_cache = Arc::new(Mutex::new(HashMap::<String, OffsetDateTime>::new()));
    let rate_limits = Arc::new(Mutex::new(HashMap::<String, RegistryRateLimitBucket>::new()));
    loop {
        let (mut stream, peer_addr) = listener.accept()?;
        let entries = Arc::clone(&entries);
        let replay_cache = Arc::clone(&replay_cache);
        let rate_limits = Arc::clone(&rate_limits);
        let ttl_sec = args.ttl_sec;

        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break None;
            }
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break Some(position + 4);
            }
        };
        let Some(header_end) = header_end else {
            continue;
        };
        let headers = String::from_utf8(request[..header_end].to_vec())?;
        let request_line = headers
            .lines()
            .next()
            .ok_or_else(|| anyhow!("registry request missing request line"))?;
        let header_map = parse_http_headers(&headers);
        let content_length = header_map
            .get("content-length")
            .map(|value| value.parse::<usize>())
            .transpose()?
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let body =
            &request[header_end..header_end + content_length.min(request.len() - header_end)];
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .ok_or_else(|| anyhow!("registry request missing method"))?;
        let path = request_parts
            .next()
            .ok_or_else(|| anyhow!("registry request missing path"))?;
        let now = OffsetDateTime::now_utc();

        let dispatch_route = |method: &str, path: &str, body: &[u8]| -> Result<HttpResponse> {
            match (method, path) {
                ("GET", "/peers") => {
                    let cutoff = now - TimeDuration::seconds(ttl_sec as i64);
                    let mut guard = entries.lock().expect("registry entries");
                    guard.retain(|_, record| record.published_at >= cutoff);
                    let response = guard.values().cloned().collect::<Vec<_>>();
                    Ok((
                        "HTTP/1.1 200 OK",
                        Vec::new(),
                        serde_json::to_vec(&response)?,
                    ))
                }
                ("POST", "/announce") => {
                    let record = serde_json::from_slice::<RegistryPeerRecord>(body)?;
                    entries
                        .lock()
                        .expect("registry entries")
                        .insert(record.actor_id.clone(), record);
                    Ok(("HTTP/1.1 200 OK", Vec::new(), br#"{"ok":true}"#.to_vec()))
                }
                _ => Ok((
                    "HTTP/1.1 404 Not Found",
                    Vec::new(),
                    br#"{"error":"not found"}"#.to_vec(),
                )),
            }
        };

        let unauthorized_response = |error: &anyhow::Error| -> Result<HttpResponse> {
            Ok((
                "HTTP/1.1 401 Unauthorized",
                Vec::new(),
                serde_json::to_vec(
                    &serde_json::json!({"error": "unauthorized", "detail": error.to_string()}),
                )?,
            ))
        };

        let require_auth = |body: &[u8]| -> Result<HttpResponse> {
            let secret = shared_secret
                .as_deref()
                .expect("shared_secret checked before call");
            match validate_registry_auth_headers(secret, method, path, &header_map, body, now) {
                Ok(validated) => {
                    match remember_registry_nonce(
                        &mut replay_cache.lock().expect("registry replay cache"),
                        &validated.nonce,
                        now,
                    ) {
                        Ok(()) => dispatch_route(method, path, body),
                        Err(error) => unauthorized_response(&error),
                    }
                }
                Err(error) => unauthorized_response(&error),
            }
        };

        let (status_line, response_headers, response_body) =
            if let Some(limit) = registry_rate_limit_for(&args, method, path) {
                let rate_limit_key = format!("{}:{method}:{path}", peer_addr.ip());
                if let Some(retry_after) = enforce_registry_rate_limit(
                    &mut rate_limits.lock().expect("registry rate limits"),
                    &rate_limit_key,
                    limit,
                    args.rate_limit_window_sec,
                    now,
                ) {
                    (
                        "HTTP/1.1 429 Too Many Requests",
                        vec![("Retry-After", retry_after.to_string())],
                        serde_json::to_vec(&serde_json::json!({
                            "error": "rate_limited",
                            "detail": "registry rate limit exceeded"
                        }))?,
                    )
                } else if shared_secret.is_some() {
                    require_auth(body)?
                } else {
                    dispatch_route(method, path, body)?
                }
            } else if shared_secret.is_some() {
                require_auth(body)?
            } else {
                dispatch_route(method, path, body)?
            };

        write!(
            stream,
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            response_body.len()
        )?;
        for (name, value) in response_headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
        write!(stream, "\r\n")?;
        stream.write_all(&response_body)?;
        stream.flush()?;
    }
}

// ---------------------------------------------------------------------------
// Registry sync (client side)
// ---------------------------------------------------------------------------

pub(crate) fn sync_discovery_registry(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: Option<&RuntimeTopology>,
    transport: Option<&RuntimeTransport>,
) -> Result<()> {
    if !config.discovery.auto_discovery {
        return Ok(());
    }
    let Some(registry_url) = config.discovery.registry_url.as_deref() else {
        return Ok(());
    };
    let shared_secret = resolve_registry_shared_secret(config)?;
    let identity = match store.local_identity()? {
        Some(identity) => identity,
        None => return Ok(()),
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    if let (Some(topology), Some(transport)) = (topology, transport) {
        let announcement = RegistryPeerRecord {
            actor_id: identity.actor_id.to_string(),
            node_id: identity.node_id.to_string(),
            public_key: identity.public_key.clone(),
            stop_public_key: local_stop_public_key_helper(config, paths),
            capabilities: local_advertised_capabilities(config),
            listen_addresses: local_listen_addresses(topology, transport),
            role: config.node.role.to_string(),
            published_at: OffsetDateTime::now_utc(),
        };
        let announce_url = registry_endpoint(registry_url, "announce");
        let announcement_body = serde_json::to_vec(&announcement)?;
        apply_registry_auth(
            client
                .post(&announce_url)
                .header("content-type", "application/json")
                .body(announcement_body.clone()),
            shared_secret.as_deref(),
            Method::POST,
            &announce_url,
            &announcement_body,
        )?
        .send()?
        .error_for_status()?;
    }

    let peers_url = registry_endpoint(registry_url, "peers");
    let response = apply_registry_auth(
        client.get(&peers_url),
        shared_secret.as_deref(),
        Method::GET,
        &peers_url,
        &[],
    )?
    .send()?
    .error_for_status()?
    .json::<Vec<RegistryPeerRecord>>()?;
    for record in response {
        if record.actor_id == identity.actor_id.as_str() {
            continue;
        }
        upsert_bootstrap_peer(BootstrapPeerParams {
            store,
            actor_id: &ActorId::new(record.actor_id.clone())?,
            node_id: NodeId::new(record.node_id.clone())?,
            public_key: record.public_key,
            stop_public_key: record.stop_public_key,
            capabilities: record.capabilities,
            listen_addresses: &record.listen_addresses,
            seen_at: record.published_at,
        })?;
    }
    if config.discovery.registry_ttl_sec > 0 {
        let cutoff = OffsetDateTime::now_utc()
            - TimeDuration::seconds(config.discovery.registry_ttl_sec as i64);
        store.purge_stale_peer_addresses(cutoff)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DataPaths, NodeRole};
    use starweft_id::{ActorId, NodeId};
    use starweft_p2p::{RuntimeTopology, RuntimeTransport};
    use starweft_store::{PeerAddressRecord, Store};
    use tempfile::TempDir;
    use time::{Duration as TimeDuration, OffsetDateTime};

    #[test]
    fn sync_discovery_registry_announces_and_imports_peers() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind registry");
        let address = listener.local_addr().expect("registry addr");
        let shared_secret = "registry-test-secret".to_owned();
        let discovered_actor = ActorId::generate().to_string();
        let discovered_node = NodeId::generate().to_string();
        let discovered_actor_for_server = discovered_actor.clone();
        let discovered_node_for_server = discovered_node.clone();
        let shared_secret_for_server = shared_secret.clone();
        let server = std::thread::spawn(move || {
            let (mut post_stream, _) = listener.accept().expect("accept announce");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let header_end = loop {
                let read = post_stream.read(&mut chunk).expect("read announce");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).expect("utf8 headers");
            let header_map = parse_http_headers(&headers);
            assert!(header_map.contains_key(REGISTRY_NONCE_HEADER));
            let content_length = header_map
                .get("content-length")
                .expect("content-length")
                .parse::<usize>()
                .expect("content-length value");
            while request.len() < header_end + content_length {
                let read = post_stream.read(&mut chunk).expect("read announce body");
                request.extend_from_slice(&chunk[..read]);
            }
            assert!(headers.starts_with("POST /announce HTTP/1.1"));
            validate_registry_auth_headers(
                &shared_secret_for_server,
                "POST",
                "/announce",
                &header_map,
                &request[header_end..header_end + content_length],
                OffsetDateTime::now_utc(),
            )
            .expect("validate announce auth");
            write!(
                post_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{{\"ok\":true}}"
            )
            .expect("write announce response");

            let (mut get_stream, _) = listener.accept().expect("accept peers");
            let mut request = Vec::new();
            let header_end = loop {
                let read = get_stream.read(&mut chunk).expect("read peers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).expect("utf8 headers");
            assert!(headers.starts_with("GET /peers HTTP/1.1"));
            let header_map = parse_http_headers(&headers);
            assert!(header_map.contains_key(REGISTRY_NONCE_HEADER));
            validate_registry_auth_headers(
                &shared_secret_for_server,
                "GET",
                "/peers",
                &header_map,
                &[],
                OffsetDateTime::now_utc(),
            )
            .expect("validate peers auth");

            let response = vec![RegistryPeerRecord {
                actor_id: discovered_actor_for_server.clone(),
                node_id: discovered_node_for_server.clone(),
                public_key: "registry-peer-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                listen_addresses: vec!["/unix/registry-peer.sock".to_owned()],
                role: "worker".to_owned(),
                published_at: OffsetDateTime::now_utc(),
            }];
            let body = serde_json::to_string(&response).expect("response json");
            write!(
                get_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write peers response");
        });

        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path().join("owner");
        let paths = DataPaths::from_root(&data_dir);
        let mut config = Config::for_role(NodeRole::Owner, &data_dir, None);
        config.discovery.registry_url = Some(format!("http://{address}"));
        config.discovery.registry_shared_secret = Some(shared_secret);
        paths.ensure_layout().expect("layout");
        let store = Store::open(&paths.ledger_db).expect("store");
        let keypair = starweft_crypto::StoredKeypair::generate();
        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: ActorId::generate(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: keypair.public_key.clone(),
                private_key_ref: paths.actor_key.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert local identity");
        let stale_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: stale_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/stale.sock".to_owned(),
                last_seen_at: Some(OffsetDateTime::now_utc() - TimeDuration::seconds(600)),
            })
            .expect("insert stale peer");
        let manual_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: manual_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/manual.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("insert manual peer");
        let topology = RuntimeTopology::validate(
            ["/unix/owner.sock".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = RuntimeTransport::local_mailbox();

        sync_discovery_registry(&config, &paths, &store, Some(&topology), Some(&transport))
            .expect("sync discovery registry");
        server.join().expect("join server");

        let peer = store
            .peer_identity(&ActorId::new(discovered_actor).expect("actor id"))
            .expect("peer identity")
            .expect("peer record");
        assert_eq!(peer.capabilities, vec!["openclaw.execution.v1".to_owned()]);
        let peers = store.list_peer_addresses().expect("list peers");
        assert!(peers.iter().any(|peer| peer.actor_id == manual_actor));
        assert!(!peers.iter().any(|peer| peer.actor_id == stale_actor));
    }

    #[test]
    fn validate_registry_auth_headers_rejects_stale_timestamp() {
        let secret = "registry-test-secret";
        let headers = build_registry_auth_headers(
            secret,
            "GET",
            "/peers",
            &[],
            OffsetDateTime::now_utc() - TimeDuration::seconds(REGISTRY_AUTH_MAX_SKEW_SEC + 1),
        )
        .expect("build auth headers");
        let header_map = HashMap::from([
            (
                REGISTRY_AUTH_HEADER.to_owned(),
                REGISTRY_AUTH_SCHEME.to_owned(),
            ),
            (
                REGISTRY_TIMESTAMP_HEADER.to_owned(),
                headers.timestamp.clone(),
            ),
            (REGISTRY_NONCE_HEADER.to_owned(), headers.nonce.clone()),
            (
                REGISTRY_CONTENT_SHA256_HEADER.to_owned(),
                headers.content_sha256.clone(),
            ),
            (
                REGISTRY_SIGNATURE_HEADER.to_owned(),
                headers.signature.clone(),
            ),
        ]);

        let error = validate_registry_auth_headers(
            secret,
            "GET",
            "/peers",
            &header_map,
            &[],
            OffsetDateTime::now_utc(),
        )
        .expect_err("stale timestamp must be rejected");
        assert!(error.to_string().contains("timestamp drift"));
    }

    #[test]
    fn remember_registry_nonce_rejects_replay_within_auth_window() {
        let now = OffsetDateTime::now_utc();
        let mut replay_cache = HashMap::new();

        remember_registry_nonce(&mut replay_cache, "nonce-01", now).expect("record nonce");
        let error = remember_registry_nonce(&mut replay_cache, "nonce-01", now)
            .expect_err("replayed nonce must be rejected");
        assert!(error.to_string().contains("replayed registry nonce"));

        remember_registry_nonce(
            &mut replay_cache,
            "nonce-02",
            now + TimeDuration::seconds(REGISTRY_AUTH_MAX_SKEW_SEC + 1),
        )
        .expect("cleanup expired nonce");
        assert!(!replay_cache.contains_key("nonce-01"));
    }

    #[test]
    fn enforce_registry_rate_limit_rejects_burst_with_retry_after() {
        let now = OffsetDateTime::now_utc();
        let mut rate_limits = HashMap::<String, RegistryRateLimitBucket>::new();

        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 2, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 2, 60, now),
            None
        );
        let retry_after = enforce_registry_rate_limit(
            &mut rate_limits,
            "127.0.0.1:GET:/peers",
            2,
            60,
            now + TimeDuration::seconds(5),
        )
        .expect("third request should be limited");
        assert_eq!(retry_after, 55);
    }

    #[test]
    fn enforce_registry_rate_limit_resets_per_window_and_endpoint() {
        let now = OffsetDateTime::now_utc();
        let mut rate_limits = HashMap::<String, RegistryRateLimitBucket>::new();

        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:POST:/announce", 1, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 1, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(
                &mut rate_limits,
                "127.0.0.1:POST:/announce",
                1,
                60,
                now + TimeDuration::seconds(61),
            ),
            None
        );
    }
}
