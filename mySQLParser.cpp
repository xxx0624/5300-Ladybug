/**
 * @file   mySQLParser.cpp
 * @brief  Implements the interfaces of mySQLParser.h
 * 
 * This contains a lot of methods used to parse kinds of statements from hsql lib
 *
 * @authors Ethan Guttman, XingZheng
 */

#include <sstream>
#include <string>
#include <stdio.h>
#include "mySQLParser.h"
using namespace hsql;
using namespace std;


namespace myhsql{

    /** @brief convert Operator Expr statement into string
    *  @param  Operator Expr statement
    *  @return string
    */
    string operatorExpressionToString(Expr* expr){
        if(expr == NULL){
            return "NULL";
        }

        stringstream res;
        if(expr->opType == Expr::NOT){
            res << "NOT";
        }
        res << exprToString(expr->expr) << " ";
        switch(expr->opType){
        case Expr::SIMPLE_OP:
            res << expr->opChar;
            break;
        case Expr::AND:
            res << "AND";
            break;
        case Expr::OR:
            res <<  "OR";
            break;
        default:
            break;
        }
        if(expr->expr2 != NULL){
            res << " " << exprToString(expr->expr2);
        }
        return res.str();
    }

    /** @brief convert TableRef statement into string
    *  @param  TableRef statement
    *  @return string
    */
    string tableRefToString(TableRef* table){
        stringstream res;
        switch (table->type) {
            case kTableName:
                res << table->name;
                if (table->alias != NULL) {
                    res << " AS " << table->alias;
                }
                break;
            case kTableSelect:
                res << selectStatementToString(table->select);
                break;
            case kTableJoin:
                res << tableRefToString(table->join->left);
                switch (table->join->type){
                    case kJoinCross:
                    case kJoinInner:
                        res << " JOIN ";
                        break;
                    case kJoinOuter:
                    case kJoinLeftOuter:
                    case kJoinLeft:
                        res << " LEFT JOIN ";
                        break;
                    case kJoinRightOuter:
                    case kJoinRight:
                        res << " RIGHT JOIN ";
                        break;
                    case kJoinNatural:
                        res << " NATURAL JOIN ";
                        break; 
                }
                res << tableRefToString(table->join->right);
                if(table->join->condition != NULL)
                    res << " ON " << exprToString(table->join->condition);
                break;
            case kTableCrossProduct:
                int idx = 0;
                for (TableRef* tbl : *table->list) {
                    if(idx == 0){
                        res << tableRefToString(tbl);
                    } else {
                        res << ", " << tableRefToString(tbl);
                    }
                    idx ++;
                }
                break;
        }
        return res.str();
    }
	
	/** @brief convert ColumnDefinition into string
    *  @param  ColumnDefinition variable
    *  @return string
    */
	string columnDefToString(const ColumnDefinition *col){
		string ret = col->name;
		ret += " ";
		switch (col->type){
			case ColumnDefinition::INT:
				ret += "INT";
				break;
			case ColumnDefinition::DOUBLE:
				ret += "DOUBLE";
				break;
			case ColumnDefinition::TEXT:
				ret += "TEXT";
				break;
			default:
				ret += "...";
				break;
		}
		return ret;
	}
	
	/** @brief convert Create statement into string
    *  @param  Create statement
    *  @return string
    */
    string createStatementToString(const CreateStatement* stmt){
		stringstream res;
		res << "CREATE TABLE " << stmt->tableName << " (";
		int idx = stmt->columns->size();
		for(ColumnDefinition *col : *stmt->columns){
			res << columnDefToString(col);
			if(idx > 1){
				res << ", ";
			}
			idx--;
        }
        res << ")";
        return res.str();
    }

    /** @brief convert Expr statement into string
    *  @param  Expr statement
    *  @return string
    */
    string exprToString(Expr* expr){
        stringstream res;
        switch (expr->type) {
            case kExprStar:
                res << "*";
                break;
            case kExprColumnRef:
                if (expr->table != NULL){
                    res << expr->table << ".";
                }
            case kExprLiteralString:
                res << expr->name;
                break;
            case kExprLiteralFloat:
                res << to_string(expr->fval);
                break;
            case kExprLiteralInt:
                res << to_string(expr->ival);
                break;
            case kExprFunctionRef:
                res << expr->name << "?" << expr->expr->name;
                break;
            case kExprOperator:
                res << operatorExpressionToString(expr);
                break;
            default:
                fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
        }
        if (expr->alias != NULL) {
            res << " AS " << expr->alias;
        }
        return res.str();
    }

    /** @brief convert Select statement into string
    *  @param  Select statement
    *  @return string
    */
    string selectStatementToString(const SelectStatement* stmt){
        stringstream res;
        res << "SELECT" << " ";
        int idx = stmt->selectList->size();
        for(Expr* expr : *stmt->selectList){
            res << exprToString(expr);
            if(idx > 1){
                res << ", ";
            }
            idx--;
        }
        res << " FROM ";
        res << tableRefToString(stmt->fromTable);
        if (stmt->whereClause != NULL) {
            res << " WHERE " << exprToString(stmt->whereClause);
        }
        return res.str();
    }

    /** @brief convert SQL statement into string
    *  @param  SQL statement
    *  @return string
    */
    string sqlStatementToString(const SQLStatement* stmt){
        switch (stmt->type()){
            case kStmtSelect:
            return selectStatementToString((const SelectStatement*) stmt);
            case kStmtCreate:
            return createStatementToString((const CreateStatement*) stmt);
            //TODO: other kinds of statements
            default:
            break;
        }
        return "";
    }
}
