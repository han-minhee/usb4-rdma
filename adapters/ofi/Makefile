CARGO ?= cargo
ROOT := $(abspath ../..)
CARGO_TARGET_DIR ?= $(ROOT)/target
export CARGO_TARGET_DIR

.PHONY: all
all:
	cd "$(ROOT)" && $(CARGO) build --locked --release -p usb4-ofi-provider --features native
	cp "$(CARGO_TARGET_DIR)/release/libusb4_fi.so" "$(CARGO_TARGET_DIR)/release/libusb4-fi.so"
