CXX = g++

CXXFLAGS = -g
#CXXFLAGS = -O3

CXXFLAGS += -Wall -Wextra -Wredundant-decls -Wdisabled-optimization -pedantic -Wctor-dtor-privacy -Wnon-virtual-dtor -Woverloaded-virtual -Wsign-promo
CXXFLAGS += -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64
CXXFLAGS += -Iosmium/include -IOSM-binary/include -I/usr/include/postgresql-9.0

LDFLAGS = -L/usr/local/lib -LOSM-binary/src -lexpat -lpthread -lpq

LIB_PROTOBUF = -lz -lprotobuf-lite -losmpbf
LIB_XML2     = `xml2-config --libs`

PROGRAMS = whosh

.PHONY: all clean

all: $(PROGRAMS)

whosh: whosh.cpp
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIB_PROTOBUF) $(LIB_XML2)

clean:
	rm -f *.o core $(PROGRAMS)

