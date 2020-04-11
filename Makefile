#CFLAGS = -std=c++11 -lstdc++ -Wall -I/usr/local/db6/include -L/usr/local/db6/lib
#all:
#	$(CXX) $(CFLAGS) sqlShell.cpp -o sqlshell -lsqlparser

sqlshell: sqlShell.o
	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

sqlShell.o : sqlShell.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<
