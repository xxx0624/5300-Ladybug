/*
 * @file sql5300.cpp main entry for relational db
 * @author Xing Zheng 
 * @author Ethan Guttman
 */

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <iostream>

#include "db_cxx.h"
#include "sqlhelper.h"
#include "SQLParser.h"
#include "mySQLParser.h"
#include "mySQLParser.cpp"

/*
 *Function to transform all characters in string to uppercase
 *@param oString the string to transform
 */
std::string stringToUpper(std::string oString){
    for(int i = 0; i < oString.length(); i++){
        oString[i] = toupper(oString[i]);
    }
    return oString;
}

/**
 *Main entry to sql5300 program
 *@args dbenvpath the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {
    //check for if the path argument exists and fail if it doesn't
	if(argc <= 1){
        std::cerr << "Usage: ./sql5300 cpsc5300/data" << std::endl;
        return EXIT_FAILURE;
    }
    const char *home = getenv("HOME");
	std::string envdir = std::string(home) + "/" + argv[1];
    std::cout << "(sqlshell: running with database environment at " + envdir + ")" << std::endl;

	//use the path argument to open up the DB environment
	//if it isn't already open
    DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
    try{
	    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException& e){
        std::cerr << "sql5300: create db env error: " << e.what() << std::endl;
        exit(1);
    }

	//main body of program that takes input and returns
	//SQL parsed text back if the input is an SQL command 
    while(true) {
        std::cout << "SQL> ";
        std::string query;
        getline(std::cin, query);
        if(query.length() == 0){
            continue;
        }
		//exit command for program is "quit"
        if(stringToUpper(query) == "QUIT"){
            std::cout << "quit" << std::endl;
            break;
        }
        // parse the given query, if invalid stop and if valid translate
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            std::cout << "Invalid SQL: " << query << std::endl;
        } else {
			//loop to handle cases where there was more than one SQL statement
			//in the input text
            for (uint i = 0; i < result->size(); ++i) {
                //hsql::printStatementInfo(result->getStatement(i));
				
				//use sqlStatementToString to transform parse result
				//into something readable and print it out
                std::cout << myhsql::sqlStatementToString(result->getStatement(i)) << std::endl;
                //std::cout << "Valid SQL" << std::endl;
            }
        }
        delete result;
    }
    env.close(0U);
    return EXIT_SUCCESS;
}
