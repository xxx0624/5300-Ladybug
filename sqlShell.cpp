#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <iostream>

#include "SQLParser.h"
#include "sqlhelper.h"
#include "db_cxx.h"

using namespace std;


string stringToUpper(string oString){
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
	string envdir = string(home) + "/" + argv[1];
    cout << "(sqlshell: running with database environment at " + envdir + ")" << endl;

    DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

    while(true) {
        cout << "SQL>";
        string query;
        getline(cin, query);
        if(stringToUpper(query) == "QUIT"){
            cout << "quit" << endl;
            env.close(0U);
            return 0;
        }
        // parse the given query
        hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "Invalid SQL: " << query << endl;
        } else {
            for (uint i = 0; i < result->size(); ++i) {
                hsql::printStatementInfo(result->getStatement(i));
            }
        }
        delete result;
    }
}
