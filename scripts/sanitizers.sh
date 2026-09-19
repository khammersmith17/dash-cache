#!/usr/bin/env bash

# Script to run both address and thread sanitizer to validate safety.
set -euo pipefail

TARGET="$(rustc -vV 2>/dev/null | awk '/^host:/ { print $2 }')"
PASS=0
FAIL=0

run() {
    local name="$1"
    shift
    echo "Running $name"
    if "$@" 2>&1; then
        echo "[$name] PASSED"
        PASS=$((PASS + 1))
    else
        echo "[$name] FAILED"
        FAIL=$((FAIL + 1))
    fi
}

run "AddressSanitizer" \
    env RUSTFLAGS="-Z sanitizer=address" \
    cargo +nightly test -Z build-std --target "$TARGET"

run "ThreadSanitizer" \
    env RUSTFLAGS="-Z sanitizer=thread -C unsafe-allow-abi-mismatch=sanitizer" \
    cargo +nightly test --lib --tests -Z build-std --target "$TARGET"



if ! [ "$FAIL" -eq 0 ]; then
    echo "Sanitizer tests failed"
    exit 1
fi
