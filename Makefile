#CFLAGS = -std=c++11 -lstdc++ -Wall -I/usr/local/db6/include -L/usr/local/db6/lib
#mysqlparser:
#	$(CXX) $(CFLAGS) -o mysqlparser mySQLParser.h mySQLParser.cpp -lsqlparser
#all:
#	$(CXX) $(CFLAGS) sqlShell.cpp -o sqlshell -lsqlparser -ldb_cxx

sqlshell: sqlShell.o mysqlparser.o
	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

sqlShell.o : sqlShell.cpp
	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -o $@ $<
