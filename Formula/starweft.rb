# typed: false
# frozen_string_literal: true

class Starweft < Formula
  desc "Distributed multi-agent task coordination CLI for P2P networks"
  homepage "https://github.com/co-r-e/starweft"
  url "https://github.com/co-r-e/starweft/archive/refs/tags/v0.3.0.tar.gz"
  sha256 "PLACEHOLDER_SHA256"
  license "MIT"
  head "https://github.com/co-r-e/starweft.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "apps/starweft")

    generate_completions_from_executable(bin/"starweft", "completions")
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/starweft --version")
  end
end
