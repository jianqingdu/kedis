CC = g++

ver=release
CFLAGS=-Wall -g -std=c++11
ifeq ($(ver), release)
CFLAGS := $(CFLAGS) -O2
endif

OS_NAME=$(shell uname)
ifeq ($(OS_NAME), Linux)
CFLAGS := $(CFLAGS) -fPIC
endif

LDFLAGS= -lpthread
AR=ar
CP=/bin/cp
RM=/bin/rm -rf

# target binary object
BIN=libbase.a

SrcDir= .
IncDir= . 
LibDir= ./

SRCS=$(foreach dir,$(SrcDir),$(wildcard $(dir)/*.cpp))
INCS=$(foreach dir,$(IncDir),$(addprefix -I,$(dir)))
LINKS=$(foreach dir,$(LibDir),$(addprefix -L,$(dir)))
CFLAGS := $(CFLAGS) $(INCS)
LDFLAGS:= $(LINKS) $(LDFLAGS)

OBJS = $(SRCS:%.cpp=%.o)
.PHONY:all clean

all:$(BIN)
$(BIN):$(OBJS)
	$(AR) -r $(BIN) $(OBJS)
	@echo " OK!\tCompile $@ "
	@echo

%.o:%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

.PHONY: clean
clean:
	@echo "[base] Cleaning files..."
	@$(RM) $(OBJS) $(BIN)
