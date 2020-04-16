#include <sstream>
#include <string>
#include <stdio.h>

#include "mySQLParser.h"

using namespace hsql;
using namespace std;

namespace myhsql{

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
                        res << " " << tableRefToString(tbl);
                    }
                    idx ++;
                }
                break;
        }
        return res.str();
    }

    string createStatementToString(const CreateStatement* stmt){
        stringstream res;
        res << "CREATE TABLE " << stmt->tableName;
        return res.str();
    }

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

    string selectStatementToString(const SelectStatement* stmt){
        stringstream res;
        res << "SELECT" << " ";
        int idx = stmt->selectList->size();
        for(Expr* expr : *stmt->selectList){
            if(idx < 2){
                res << exprToString(expr);
            } else {
              res << exprToString(expr) << ", ";
            }
            idx--;
        }
        res << " FROM ";
        res << tableRefToString(stmt->fromTable);
        if (stmt->whereClause != NULL) {
            res << "WHERE " << exprToString(stmt->whereClause);
        }
        return res.str();
    }

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
