/**
 * @file   sql5300.cpp
 * @brief  the main entry for Spint1 Project in Physical Database Class - 5300
 * @authors Ethan Guttman, XingZheng
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
#include "heap_storage.h"
using namespace std;


DbEnv *_DB_ENV;

/** @brief convert string into Uppercase
 *  @param the string needs to be converted
 *  @return updated string
 */
string stringToUpper(string oString){
    for(long unsigned int i = 0; i < oString.length(); i++){
        oString[i] = toupper(oString[i]);
    }
    return oString;
}

void init_env(string envdir){
    //use the path argument to open up the DB environment
	//if it isn't already open
    DbEnv *env = new DbEnv(0U);
    env->set_message_stream(&cout);
	env->set_error_stream(&cerr);
    try{
        env->open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException& e){
        cerr << "sql5300: create db env error: " << e.what() << endl;
        exit(1);
    }
    _DB_ENV = env;
}

/**
 *Main entry to sql5300 program
 *@args dbenvpath the path to the BerkeleyDB database environment
 */
int main(int argc, char *argv[]) {
    //check for if the path argument exists and fail if it doesn't
    if(argc <= 1){
        cerr << "Usage: ./sql5300 cpsc5300/data" << endl;
        return EXIT_FAILURE;
    }
    const char *home = getenv("HOME");
	string envdir = string(home) + "/" + argv[1];
    cout << "(sqlshell: running with database environment at " + envdir + ")" << endl;
    init_env(envdir);

	//main body of program that takes input and returns
	//SQL parsed text back if the input is an SQL command 
    while(true) {
        cout << "SQL> ";
        string query;
        getline(cin, query);
        if(query.length() == 0){
            continue;
        }
		//exit command for program is "quit"
        if(stringToUpper(query) == "QUIT"){
            cout << "quit" << endl;
            break;
        }

        if(query == "test dbblock"){
            cout << "test_slottedpage: " << (test_slottedpage() ? "ok" : "failed") << endl;
            continue;
        }

        if(query == "test dbfile"){
            cout << "test_heapfile: " << (test_heapfile() ? "ok" : "failed") << endl;
            continue;
        }

        if(query == "test dbtable"){
            cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
            continue;
        }

        // parse the given query, if invalid stop and if valid translate
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "Invalid SQL: " << query << endl;
        } else {
			//loop to handle cases where there was more than one SQL statement
			//in the input text
            for (uint i = 0; i < result->size(); ++i) {
                //hsql::printStatementInfo(result->getStatement(i));
                //use sqlStatementToString to transform parse result
				//into something readable and print it out
                cout << myhsql::sqlStatementToString(result->getStatement(i)) << endl;
                //cout << "Valid SQL" << endl;
            }
        }
        delete result;
    }
    _DB_ENV->close(0U);
    return EXIT_SUCCESS;
}
