CC=g++

ver=release
CFLAGS=-Wall -g -std=c++11
ifeq ($(ver), release)
CFLAGS += -O2
else
CFLAGS += -DDEBUG
endif

LDFLAGS= -lbase -lpthread ../hiredis/libhiredis.a ../../src/server/redis_parser.o ../../src/server/redis_byte_stream.o

RM=/bin/rm -rf
ARCH=PC

# target binary object
BIN=kedis_to_redis

SrcDir= . ../hiredis_wrapper
IncDir= ../../src/base ../../src/server ../hiredis  ../hiredis_wrapper
LibDir= ../../src/base/

SRCS=$(foreach dir,$(SrcDir),$(wildcard $(dir)/*.cpp))
INCS=$(foreach dir,$(IncDir),$(addprefix -I,$(dir)))
LINKS=$(foreach dir,$(LibDir),$(addprefix -L,$(dir)))
CFLAGS := $(CFLAGS) $(INCS)
LDFLAGS:= $(LINKS) $(LDFLAGS)

OBJS = $(SRCS:%.cpp=%.o)
.PHONY:all clean

all:$(BIN)
$(BIN):$(OBJS) ../../src/base/libbase.a ../hiredis/libhiredis.a
	$(CC) -o $(BIN) $(OBJS) $(LDFLAGS)
	@echo " OK!\tCompile $@ "
	@echo

../hiredis/libhiredis.a:
	make -C ../hiredis

%.o:%.cpp
	@echo "$(CC) $(CFLAGS) -c $< -o $@"
	@$(CC) $(CFLAGS) -c $< -o $@

.PHONY: clean
clean:
	@echo "[$(ARCH)] \tCleaning files..."
	@$(RM) $(OBJS) $(BIN)
