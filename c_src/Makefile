CXX ?= g++

LDFLAGS += -shared
CXXFLAGS += -fPIC -std=c++14 -O3 -Wall

all: ../priv/hex_nif.so

../priv/hex_nif.so: hex/hex_nif.o
	$(CXX) $? -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CFLAGS) -c $< -o $@
