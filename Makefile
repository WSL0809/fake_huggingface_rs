CARGO ?= cargo
FAKE_HUB_ROOT ?= fake_hub
RUST_LOG ?= info

.PHONY: build release run fmt clippy test start

build:
	$(CARGO) build

release:
	$(CARGO) build --release

run:
	FAKE_HUB_ROOT=$(FAKE_HUB_ROOT) RUST_LOG=$(RUST_LOG) $(CARGO) run

fmt:
	$(CARGO) fmt --all

clippy:
	$(CARGO) clippy --all-targets -- -D warnings

test:
	$(CARGO) test

start: release
	FAKE_HUB_ROOT=$(FAKE_HUB_ROOT) RUST_LOG=$(RUST_LOG) ./scripts/start_server.sh
