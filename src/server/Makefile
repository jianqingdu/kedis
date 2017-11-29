ROCKSDB_PATH=../3rd_party/rocksdb-5.6.2
ifdef rocksdb_ver
ROCKSDB_PATH=../3rd_party/rocksdb-$(rocksdb_ver)
endif

include $(ROCKSDB_PATH)/make_config.mk

CC=g++

ver=release
CFLAGS=-Wall -g -std=c++11
ifeq ($(ver), release)
CFLAGS += -O2
else
CFLAGS += -DDEBUG
endif

LDFLAGS= -lbase $(PLATFORM_LDFLAGS) $(ROCKSDB_PATH)/librocksdb.a 

LN=/bin/ln -s 
AR=ar
CP=/bin/cp
RM=/bin/rm -rf

# target binary object
BIN=kedis-server

SrcDir= . 
IncDir= ../base $(ROCKSDB_PATH)/include
LibDir= ../base/

SRCS=$(foreach dir,$(SrcDir),$(wildcard $(dir)/*.cpp))
INCS=$(foreach dir,$(IncDir),$(addprefix -I,$(dir)))
LINKS=$(foreach dir,$(LibDir),$(addprefix -L,$(dir)))
CFLAGS := $(CFLAGS) $(INCS) -Wno-deprecated-declarations
LDFLAGS:= $(LINKS) $(LDFLAGS)

OBJS = $(SRCS:%.cpp=%.o)
.PHONY:all clean

all:$(BIN)
$(BIN):$(OBJS) ../base/libbase.a $(ROCKSDB_PATH)/librocksdb.a
	$(CC) -o $(BIN) $(OBJS) $(LDFLAGS)
	@echo " OK!\tCompile $@ "
	@echo

%.o:%.cpp
	@echo "$(CC) $(CFLAGS) -c $< -o $@"
	@$(CC) $(CFLAGS) -c $< -o $@

.PHONY: clean
clean:
	@echo "[server] Cleaning files..."
	@$(RM) $(OBJS) $(BIN)
