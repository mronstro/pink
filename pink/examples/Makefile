CXX=g++
LDFLAGS= -lpthread -lrt -lprotobuf
CXXFLAGS=-O2 -std=c++14 -fno-builtin-memcmp

.PHONY: clean all

all: bg_thread http_server \
	redis_cli_test simple_http_server myredis_srv

ifndef PINK_PATH
  $(warning Warning: missing pink path, using default)
  PINK_PATH=$(CURDIR)/../..
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
	SLASH_PATH=$(CURDIR)/../third/slash
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a

RONDB_INCLUDE_DIR=$(RONDB_PATH)/include/storage/ndb
RONDB_LIBRARY=$(RONDB_PATH)/lib/libndbclient.so

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(RONDB_INCLUDE_DIR)

DEP_LIBS = $(PINK_LIBRARY) $(SLASH_LIBRARY) $(RONDB_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

bg_thread: bg_thread.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

http_server: http_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

https_server: https_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#mydispatch_srv: mydispatch_srv.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myholy_srv: myholy_srv.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myholy_srv_chandle: myholy_srv_chandle.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myproto_cli: myproto_cli.cc myproto.pb.cc
#	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

redis_cli_test: redis_cli_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

simple_http_server: simple_http_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myredis_srv: myredis_srv.cc rondis_handler.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

#myproto.pb.cc: myproto.proto
# protoc -I=. --cpp_out=. ./$^

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./bg_thread ./http_server ./https_server ./mydispatch_srv ./myholy_srv \
	./myholy_srv_chandle ./myproto_cli ./redis_cli_test ./simple_http_server ./myredis_srv
