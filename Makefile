
CFLAGS = -std=c++11 -lstdc++ -Wall -I/usr/local/db6/include -L/usr/local/db6/lib

all:
	$(CXX) $(CFLAGS) sqlShell.cpp -o sqlshell -lsqlparser

