
CFLAGS = -std=c++11 -lstdc++ -Wall -I/home/st/zhengxing/cpsc5300/sql-parser/src -L/usr/local/db6/lib

all:
	$(CXX) $(CFLAGS) sqlParser.cpp -o sqlparser -lsqlparser

