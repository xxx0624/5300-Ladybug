#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <iostream>

#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;


string stringToUpper(string oString){
    for(int i = 0; i < oString.length(); i++){
        oString[i] = toupper(oString[i]);
    }
    return oString;
}

int main(int argc, char *argv[]) {
    hsql::SQLParserResult *result;
    while(true) {
        cout << "SQL>";
        string query;
        getline(cin, query);
        if(stringToUpper(query) == "QUIT"){
            cout << "quit" << endl;
            delete result;
            return 0;
        }
        // parse the given query
        result = hsql::SQLParser::parseSQLString(query);
        if (!result->isValid()) {
            cout << "Invalid SQL: " << query << endl;
            continue;
        }
        for (uint i = 0; i < result->size(); ++i) {
            hsql::printStatementInfo(result->getStatement(i));
        }
    }
}
