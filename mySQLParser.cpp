#include <sstream>
#include <string>
#include <stdio.h>

#include "mySQLParser.h"

using namespace hsql;

namespace myhsql{

    std::string operatorExpressionToString(Expr* expr){
        if(expr == NULL){
            return "NULL";
        }
        switch(expr->opType){
        case Expr::SIMPLE_OP:
            return "" + expr->opChar;
        case Expr::AND:
            return "AND";
        case Expr::OR:
            return "OR";
        case Expr::NOT:
            return "NOT";
        default:
            return "OTHEROP";
        }
    }

    std::string tableRefToString(TableRef* table){
        std::stringstream res;
        switch (table->type) {
            case kTableName:
                res << table->name;
                break;
            case kTableSelect:
                res << selectStatementToString(table->select);
                break;
            case kTableJoin:
                res << "LEFT JOIN" << " " << tableRefToString(table->join->left) \ 
                    << " " << "RIGHT JOIN" << " " << tableRefToString(table->join->right) \
                    << " " << "ON" << " " + exprToString(table->join->condition);
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
        if (table->alias != NULL) {
            res << " ";
            res << "AS" << " " << table->alias;
        }
        return res.str();
    }

    std::string createStatementToString(const CreateStatement* stmt){
        std::stringstream res;
        res << "CREATE TABLE " << stmt->tableName;
        return res.str();
    }

    std::string exprToString(Expr* expr){
        std::string res = "";
        switch (expr->type) {
            case kExprStar:
                res = "*";
                break;
            case kExprColumnRef:
                res = expr->name;
                break;
            case kExprLiteralFloat:
                res = expr->fval;
                break;
            case kExprLiteralInt:
                res = expr->ival;
                break;
            case kExprLiteralString:
                res = expr->name;
                break;
            // TODO
            // case kExprFunctionRef:
            // inprint(expr->name, numIndent);
            // inprint(expr->expr->name, numIndent + 1);
            // break;
            case kExprOperator:
                res = operatorExpressionToString(expr);
                break;
            default:
                fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
        }
        // if (expr->alias != NULL) {
        //     inprint("Alias", numIndent + 1);
        //     inprint(expr->alias, numIndent + 2);
        // }
        return res;
    }

    std::string selectStatementToString(const SelectStatement* stmt){
        std::stringstream res;
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
        res << stmt->fromTable->name;
        //res << stmt->fromTable->getName(); works in the same way
        
        if (stmt->whereClause != NULL) {
            res << "WHERE " << exprToString(stmt->whereClause);
        }
        //TODO: other expressions
        return res.str();
    }

    std::string sqlStatementToString(const SQLStatement* stmt){
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
