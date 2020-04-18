/**
 * @file   mySQLParser.h
 * @brief  Interfaces used to parse kinds of statements
 * @author Ethan Guttman, XingZheng
 */

#ifndef __MYSQLPARSER_H__
#define __MYSQLPARSER_H__

#include "sql/statements.h"
using namespace hsql;

namespace myhsql{

    std::string operatorExpressionToString(Expr* expr);

    std::string tableRefToString(TableRef* table);
	
	std::string columnDefToString(const ColumnDefinition *col);

    std::string createStatementToString(const CreateStatement* stmt);

    std::string exprToString(Expr* expr);

    std::string selectStatementToString(const SelectStatement* stmt);

    std::string sqlStatementToString(const SQLStatement* stmt);
}

#endif
