# Generated from .\sql.g4 by ANTLR 4.7.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .sqlParser import sqlParser
else:
    from sqlParser import sqlParser

# This class defines a complete generic visitor for a parse tree produced by sqlParser.

class sqlVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by sqlParser#parse.
    def visitParse(self, ctx:sqlParser.ParseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#error.
    def visitError(self, ctx:sqlParser.ErrorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#sql_stmt_list.
    def visitSql_stmt_list(self, ctx:sqlParser.Sql_stmt_listContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#sql_stmt.
    def visitSql_stmt(self, ctx:sqlParser.Sql_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#create_database_stmt.
    def visitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#alter_database_stmt.
    def visitAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#drop_database_stmt.
    def visitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#show_databases_stmt.
    def visitShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#show_tables_stmt.
    def visitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#use_database_stmt.
    def visitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#alter_table_stmt.
    def visitAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#show_columns_stmt.
    def visitShow_columns_stmt(self, ctx:sqlParser.Show_columns_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#begin_stmt.
    def visitBegin_stmt(self, ctx:sqlParser.Begin_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#commit_stmt.
    def visitCommit_stmt(self, ctx:sqlParser.Commit_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#factored_select_stmt.
    def visitFactored_select_stmt(self, ctx:sqlParser.Factored_select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#create_index_stmt.
    def visitCreate_index_stmt(self, ctx:sqlParser.Create_index_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#create_table_stmt.
    def visitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#delete_stmt.
    def visitDelete_stmt(self, ctx:sqlParser.Delete_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#drop_index_stmt.
    def visitDrop_index_stmt(self, ctx:sqlParser.Drop_index_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#drop_table_stmt.
    def visitDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#insert_stmt.
    def visitInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#rollback_stmt.
    def visitRollback_stmt(self, ctx:sqlParser.Rollback_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#simple_select_stmt.
    def visitSimple_select_stmt(self, ctx:sqlParser.Simple_select_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#update_stmt.
    def visitUpdate_stmt(self, ctx:sqlParser.Update_stmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#column_def.
    def visitColumn_def(self, ctx:sqlParser.Column_defContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#type_name.
    def visitType_name(self, ctx:sqlParser.Type_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#column_constraint.
    def visitColumn_constraint(self, ctx:sqlParser.Column_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprFunction.
    def visitExprFunction(self, ctx:sqlParser.ExprFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprNot.
    def visitExprNot(self, ctx:sqlParser.ExprNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprLiteralValue.
    def visitExprLiteralValue(self, ctx:sqlParser.ExprLiteralValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprComparisonSecond.
    def visitExprComparisonSecond(self, ctx:sqlParser.ExprComparisonSecondContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprLike.
    def visitExprLike(self, ctx:sqlParser.ExprLikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprNotExists.
    def visitExprNotExists(self, ctx:sqlParser.ExprNotExistsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprUnaryExpr.
    def visitExprUnaryExpr(self, ctx:sqlParser.ExprUnaryExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprNotIn.
    def visitExprNotIn(self, ctx:sqlParser.ExprNotInContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprNotNull.
    def visitExprNotNull(self, ctx:sqlParser.ExprNotNullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprTableColumn.
    def visitExprTableColumn(self, ctx:sqlParser.ExprTableColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprComparisonFirst.
    def visitExprComparisonFirst(self, ctx:sqlParser.ExprComparisonFirstContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprOr.
    def visitExprOr(self, ctx:sqlParser.ExprOrContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprMul.
    def visitExprMul(self, ctx:sqlParser.ExprMulContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprAdd.
    def visitExprAdd(self, ctx:sqlParser.ExprAddContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprAnd.
    def visitExprAnd(self, ctx:sqlParser.ExprAndContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprParenthesis.
    def visitExprParenthesis(self, ctx:sqlParser.ExprParenthesisContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#exprIsNot.
    def visitExprIsNot(self, ctx:sqlParser.ExprIsNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#foreign_key_clause.
    def visitForeign_key_clause(self, ctx:sqlParser.Foreign_key_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#table_constraint.
    def visitTable_constraint(self, ctx:sqlParser.Table_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#ordering_term.
    def visitOrdering_term(self, ctx:sqlParser.Ordering_termContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#common_table_expression.
    def visitCommon_table_expression(self, ctx:sqlParser.Common_table_expressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#resultColumnAsterisk.
    def visitResultColumnAsterisk(self, ctx:sqlParser.ResultColumnAsteriskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#resultColumnTableAsterisk.
    def visitResultColumnTableAsterisk(self, ctx:sqlParser.ResultColumnTableAsteriskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#resultColumnExpr.
    def visitResultColumnExpr(self, ctx:sqlParser.ResultColumnExprContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#table_or_subquery.
    def visitTable_or_subquery(self, ctx:sqlParser.Table_or_subqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#join_clause.
    def visitJoin_clause(self, ctx:sqlParser.Join_clauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#join_operator.
    def visitJoin_operator(self, ctx:sqlParser.Join_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#join_constraint.
    def visitJoin_constraint(self, ctx:sqlParser.Join_constraintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#select_core.
    def visitSelect_core(self, ctx:sqlParser.Select_coreContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#compound_operator.
    def visitCompound_operator(self, ctx:sqlParser.Compound_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#cte_table_name.
    def visitCte_table_name(self, ctx:sqlParser.Cte_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#signed_number.
    def visitSigned_number(self, ctx:sqlParser.Signed_numberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#literal_value.
    def visitLiteral_value(self, ctx:sqlParser.Literal_valueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#unary_operator.
    def visitUnary_operator(self, ctx:sqlParser.Unary_operatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#error_message.
    def visitError_message(self, ctx:sqlParser.Error_messageContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#module_argument.
    def visitModule_argument(self, ctx:sqlParser.Module_argumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#column_alias.
    def visitColumn_alias(self, ctx:sqlParser.Column_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#keyword.
    def visitKeyword(self, ctx:sqlParser.KeywordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#name.
    def visitName(self, ctx:sqlParser.NameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#function_name.
    def visitFunction_name(self, ctx:sqlParser.Function_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#database_name.
    def visitDatabase_name(self, ctx:sqlParser.Database_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#table_name.
    def visitTable_name(self, ctx:sqlParser.Table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#table_or_index_name.
    def visitTable_or_index_name(self, ctx:sqlParser.Table_or_index_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#new_table_name.
    def visitNew_table_name(self, ctx:sqlParser.New_table_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#new_database_name.
    def visitNew_database_name(self, ctx:sqlParser.New_database_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#column_name.
    def visitColumn_name(self, ctx:sqlParser.Column_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#collation_name.
    def visitCollation_name(self, ctx:sqlParser.Collation_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#foreign_table.
    def visitForeign_table(self, ctx:sqlParser.Foreign_tableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#index_name.
    def visitIndex_name(self, ctx:sqlParser.Index_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#trigger_name.
    def visitTrigger_name(self, ctx:sqlParser.Trigger_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#view_name.
    def visitView_name(self, ctx:sqlParser.View_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#module_name.
    def visitModule_name(self, ctx:sqlParser.Module_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#table_alias.
    def visitTable_alias(self, ctx:sqlParser.Table_aliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#transaction_name.
    def visitTransaction_name(self, ctx:sqlParser.Transaction_nameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by sqlParser#any_name.
    def visitAny_name(self, ctx:sqlParser.Any_nameContext):
        return self.visitChildren(ctx)



del sqlParser