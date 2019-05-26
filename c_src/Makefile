CXX ?= g++

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~s/erts-~s/include/\", [code:root_dir(), erlang:system_info(version)]).")

LDFLAGS += -shared
CXXFLAGS += -fPIC -std=c++14 -O3 -Wall -isystem $(ERTS_INCLUDE_DIR)

all: ../priv/hex_nif.so

../priv/hex_nif.so: hex/hex_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CFLAGS) -c $< -o $@
