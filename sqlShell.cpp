#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <iostream>

#include "db_cxx.h"
#include "sqlhelper.h"
#include "SQLParser.h"
#include "mySQLParser.h"
#include "mySQLParser.cpp"

std::string stringToUpper(std::string oString){
    for(int i = 0; i < oString.length(); i++){
        oString[i] = toupper(oString[i]);
    }
    return oString;
}

int main(int argc, char *argv[]) {
    if(argc <= 1){
        fprintf(stderr, "Usage: ./sqlshell cpsc5300/data\n");
        return -1;
    }
    const char *home = getenv("HOME");
	std::string envdir = std::string(home) + "/" + argv[1];
    std::cout << "(sqlshell: running with database environment at " + envdir + ")" << std::endl;

    DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    while(true) {
        std::cout << "SQL>";
        std::string query;
        getline(std::cin, query);
        if(stringToUpper(query) == "QUIT"){
            std::cout << "quit" << std::endl;
            env.close(0U);
            return 0;
        }
        // parse the given query
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            std::cout << "Invalid SQL: " << query << std::endl;
        } else {
            for (uint i = 0; i < result->size(); ++i) {
                hsql::printStatementInfo(result->getStatement(i));
                std::cout << myhsql::sqlStatementToString(result->getStatement(i)) << std::endl;
            }
        }
        delete result;
    }
}
