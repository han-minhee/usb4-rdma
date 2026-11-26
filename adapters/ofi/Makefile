CARGO ?= cargo
PKG_CONFIG ?= pkg-config
ROOT := $(abspath ../..)
CARGO_TARGET_DIR ?= $(ROOT)/target
export CARGO_TARGET_DIR
LIBFABRIC_CFLAGS := $(shell $(PKG_CONFIG) --cflags libfabric)
LIBFABRIC_LIBS := $(shell $(PKG_CONFIG) --libs libfabric)
ARTIFACT := $(CARGO_TARGET_DIR)/release/libusb4-fi.so

.PHONY: all
all:
	$(PKG_CONFIG) --atleast-version=2.4.0 libfabric
	cd "$(ROOT)" && $(CARGO) build --locked --release -p usb4-rdma-ofi
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LIBFABRIC_CFLAGS) -O3 -std=c11 -Wall -Wextra -Werror -fPIC -shared \
		-Wl,--exclude-libs,ALL -Wl,-z,defs $(LDFLAGS) -o "$(ARTIFACT)" provider.c \
		"$(CARGO_TARGET_DIR)/release/libusb4_rdma_ofi.a" $(LIBFABRIC_LIBS) -lpthread -ldl -lm
