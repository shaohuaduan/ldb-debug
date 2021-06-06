include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: simple_example, ldb

simple_example: librocksdb simple_example.cc
	$(CXX) $(CXXFLAGS) simple_example.cc  -o simple_example  ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

ldb: librocksdb ldb.cc ldb_tool.cc ldb_cmd.cc ldb_tool.h ldb_cmd.h ldb_cmd_execute_result.h 
	$(CXX) $(CXXFLAGS) ldb.cc ldb_tool.cc ldb_cmd.cc ldb_tool.h ldb_cmd.h ldb_cmd_execute_result.h -o ldb  ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./simple_example, ./ldb

librocksdb:
	cd .. && $(MAKE) static_lib
