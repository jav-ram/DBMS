# Generated from .\sql.g4 by ANTLR 4.7.1
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .sqlParser import sqlParser
else:
    from sqlParser import sqlParser

# This class defines a complete listener for a parse tree produced by sqlParser.
class sqlListener(ParseTreeListener):

    # Enter a parse tree produced by sqlParser#parse.
    def enterParse(self, ctx:sqlParser.ParseContext):
        pass

    # Exit a parse tree produced by sqlParser#parse.
    def exitParse(self, ctx:sqlParser.ParseContext):
        pass


    # Enter a parse tree produced by sqlParser#error.
    def enterError(self, ctx:sqlParser.ErrorContext):
        pass

    # Exit a parse tree produced by sqlParser#error.
    def exitError(self, ctx:sqlParser.ErrorContext):
        pass


    # Enter a parse tree produced by sqlParser#sql_stmt_list.
    def enterSql_stmt_list(self, ctx:sqlParser.Sql_stmt_listContext):
        pass

    # Exit a parse tree produced by sqlParser#sql_stmt_list.
    def exitSql_stmt_list(self, ctx:sqlParser.Sql_stmt_listContext):
        pass


    # Enter a parse tree produced by sqlParser#sql_stmt.
    def enterSql_stmt(self, ctx:sqlParser.Sql_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#sql_stmt.
    def exitSql_stmt(self, ctx:sqlParser.Sql_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#create_database_stmt.
    def enterCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#create_database_stmt.
    def exitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#alter_database_stmt.
    def enterAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#alter_database_stmt.
    def exitAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#drop_database_stmt.
    def enterDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#drop_database_stmt.
    def exitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#show_databases_stmt.
    def enterShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#show_databases_stmt.
    def exitShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#show_tables_stmt.
    def enterShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#show_tables_stmt.
    def exitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#use_database_stmt.
    def enterUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#use_database_stmt.
    def exitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#alter_table_stmt.
    def enterAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#alter_table_stmt.
    def exitAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#alterRenameTo.
    def enterAlterRenameTo(self, ctx:sqlParser.AlterRenameToContext):
        pass

    # Exit a parse tree produced by sqlParser#alterRenameTo.
    def exitAlterRenameTo(self, ctx:sqlParser.AlterRenameToContext):
        pass


    # Enter a parse tree produced by sqlParser#alterAddColumn.
    def enterAlterAddColumn(self, ctx:sqlParser.AlterAddColumnContext):
        pass

    # Exit a parse tree produced by sqlParser#alterAddColumn.
    def exitAlterAddColumn(self, ctx:sqlParser.AlterAddColumnContext):
        pass


    # Enter a parse tree produced by sqlParser#alterAddConstraint.
    def enterAlterAddConstraint(self, ctx:sqlParser.AlterAddConstraintContext):
        pass

    # Exit a parse tree produced by sqlParser#alterAddConstraint.
    def exitAlterAddConstraint(self, ctx:sqlParser.AlterAddConstraintContext):
        pass


    # Enter a parse tree produced by sqlParser#alterDropColumn.
    def enterAlterDropColumn(self, ctx:sqlParser.AlterDropColumnContext):
        pass

    # Exit a parse tree produced by sqlParser#alterDropColumn.
    def exitAlterDropColumn(self, ctx:sqlParser.AlterDropColumnContext):
        pass


    # Enter a parse tree produced by sqlParser#alterDropConstraint.
    def enterAlterDropConstraint(self, ctx:sqlParser.AlterDropConstraintContext):
        pass

    # Exit a parse tree produced by sqlParser#alterDropConstraint.
    def exitAlterDropConstraint(self, ctx:sqlParser.AlterDropConstraintContext):
        pass


    # Enter a parse tree produced by sqlParser#show_columns_stmt.
    def enterShow_columns_stmt(self, ctx:sqlParser.Show_columns_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#show_columns_stmt.
    def exitShow_columns_stmt(self, ctx:sqlParser.Show_columns_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#begin_stmt.
    def enterBegin_stmt(self, ctx:sqlParser.Begin_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#begin_stmt.
    def exitBegin_stmt(self, ctx:sqlParser.Begin_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#commit_stmt.
    def enterCommit_stmt(self, ctx:sqlParser.Commit_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#commit_stmt.
    def exitCommit_stmt(self, ctx:sqlParser.Commit_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#factored_select_stmt.
    def enterFactored_select_stmt(self, ctx:sqlParser.Factored_select_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#factored_select_stmt.
    def exitFactored_select_stmt(self, ctx:sqlParser.Factored_select_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#create_index_stmt.
    def enterCreate_index_stmt(self, ctx:sqlParser.Create_index_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#create_index_stmt.
    def exitCreate_index_stmt(self, ctx:sqlParser.Create_index_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#create_table_stmt.
    def enterCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#create_table_stmt.
    def exitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#delete_stmt.
    def enterDelete_stmt(self, ctx:sqlParser.Delete_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#delete_stmt.
    def exitDelete_stmt(self, ctx:sqlParser.Delete_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#drop_index_stmt.
    def enterDrop_index_stmt(self, ctx:sqlParser.Drop_index_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#drop_index_stmt.
    def exitDrop_index_stmt(self, ctx:sqlParser.Drop_index_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#drop_table_stmt.
    def enterDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#drop_table_stmt.
    def exitDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#insert_stmt.
    def enterInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#insert_stmt.
    def exitInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#rollback_stmt.
    def enterRollback_stmt(self, ctx:sqlParser.Rollback_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#rollback_stmt.
    def exitRollback_stmt(self, ctx:sqlParser.Rollback_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#simple_select_stmt.
    def enterSimple_select_stmt(self, ctx:sqlParser.Simple_select_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#simple_select_stmt.
    def exitSimple_select_stmt(self, ctx:sqlParser.Simple_select_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#update_stmt.
    def enterUpdate_stmt(self, ctx:sqlParser.Update_stmtContext):
        pass

    # Exit a parse tree produced by sqlParser#update_stmt.
    def exitUpdate_stmt(self, ctx:sqlParser.Update_stmtContext):
        pass


    # Enter a parse tree produced by sqlParser#column_def.
    def enterColumn_def(self, ctx:sqlParser.Column_defContext):
        pass

    # Exit a parse tree produced by sqlParser#column_def.
    def exitColumn_def(self, ctx:sqlParser.Column_defContext):
        pass


    # Enter a parse tree produced by sqlParser#type_name.
    def enterType_name(self, ctx:sqlParser.Type_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#type_name.
    def exitType_name(self, ctx:sqlParser.Type_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#column_constraint.
    def enterColumn_constraint(self, ctx:sqlParser.Column_constraintContext):
        pass

    # Exit a parse tree produced by sqlParser#column_constraint.
    def exitColumn_constraint(self, ctx:sqlParser.Column_constraintContext):
        pass


    # Enter a parse tree produced by sqlParser#exprFunction.
    def enterExprFunction(self, ctx:sqlParser.ExprFunctionContext):
        pass

    # Exit a parse tree produced by sqlParser#exprFunction.
    def exitExprFunction(self, ctx:sqlParser.ExprFunctionContext):
        pass


    # Enter a parse tree produced by sqlParser#exprNot.
    def enterExprNot(self, ctx:sqlParser.ExprNotContext):
        pass

    # Exit a parse tree produced by sqlParser#exprNot.
    def exitExprNot(self, ctx:sqlParser.ExprNotContext):
        pass


    # Enter a parse tree produced by sqlParser#exprLiteralValue.
    def enterExprLiteralValue(self, ctx:sqlParser.ExprLiteralValueContext):
        pass

    # Exit a parse tree produced by sqlParser#exprLiteralValue.
    def exitExprLiteralValue(self, ctx:sqlParser.ExprLiteralValueContext):
        pass


    # Enter a parse tree produced by sqlParser#exprComparisonSecond.
    def enterExprComparisonSecond(self, ctx:sqlParser.ExprComparisonSecondContext):
        pass

    # Exit a parse tree produced by sqlParser#exprComparisonSecond.
    def exitExprComparisonSecond(self, ctx:sqlParser.ExprComparisonSecondContext):
        pass


    # Enter a parse tree produced by sqlParser#exprLike.
    def enterExprLike(self, ctx:sqlParser.ExprLikeContext):
        pass

    # Exit a parse tree produced by sqlParser#exprLike.
    def exitExprLike(self, ctx:sqlParser.ExprLikeContext):
        pass


    # Enter a parse tree produced by sqlParser#exprNotExists.
    def enterExprNotExists(self, ctx:sqlParser.ExprNotExistsContext):
        pass

    # Exit a parse tree produced by sqlParser#exprNotExists.
    def exitExprNotExists(self, ctx:sqlParser.ExprNotExistsContext):
        pass


    # Enter a parse tree produced by sqlParser#exprUnaryExpr.
    def enterExprUnaryExpr(self, ctx:sqlParser.ExprUnaryExprContext):
        pass

    # Exit a parse tree produced by sqlParser#exprUnaryExpr.
    def exitExprUnaryExpr(self, ctx:sqlParser.ExprUnaryExprContext):
        pass


    # Enter a parse tree produced by sqlParser#exprNotIn.
    def enterExprNotIn(self, ctx:sqlParser.ExprNotInContext):
        pass

    # Exit a parse tree produced by sqlParser#exprNotIn.
    def exitExprNotIn(self, ctx:sqlParser.ExprNotInContext):
        pass


    # Enter a parse tree produced by sqlParser#exprNotNull.
    def enterExprNotNull(self, ctx:sqlParser.ExprNotNullContext):
        pass

    # Exit a parse tree produced by sqlParser#exprNotNull.
    def exitExprNotNull(self, ctx:sqlParser.ExprNotNullContext):
        pass


    # Enter a parse tree produced by sqlParser#exprTableColumn.
    def enterExprTableColumn(self, ctx:sqlParser.ExprTableColumnContext):
        pass

    # Exit a parse tree produced by sqlParser#exprTableColumn.
    def exitExprTableColumn(self, ctx:sqlParser.ExprTableColumnContext):
        pass


    # Enter a parse tree produced by sqlParser#exprComparisonFirst.
    def enterExprComparisonFirst(self, ctx:sqlParser.ExprComparisonFirstContext):
        pass

    # Exit a parse tree produced by sqlParser#exprComparisonFirst.
    def exitExprComparisonFirst(self, ctx:sqlParser.ExprComparisonFirstContext):
        pass


    # Enter a parse tree produced by sqlParser#exprOr.
    def enterExprOr(self, ctx:sqlParser.ExprOrContext):
        pass

    # Exit a parse tree produced by sqlParser#exprOr.
    def exitExprOr(self, ctx:sqlParser.ExprOrContext):
        pass


    # Enter a parse tree produced by sqlParser#exprMul.
    def enterExprMul(self, ctx:sqlParser.ExprMulContext):
        pass

    # Exit a parse tree produced by sqlParser#exprMul.
    def exitExprMul(self, ctx:sqlParser.ExprMulContext):
        pass


    # Enter a parse tree produced by sqlParser#exprAdd.
    def enterExprAdd(self, ctx:sqlParser.ExprAddContext):
        pass

    # Exit a parse tree produced by sqlParser#exprAdd.
    def exitExprAdd(self, ctx:sqlParser.ExprAddContext):
        pass


    # Enter a parse tree produced by sqlParser#exprAnd.
    def enterExprAnd(self, ctx:sqlParser.ExprAndContext):
        pass

    # Exit a parse tree produced by sqlParser#exprAnd.
    def exitExprAnd(self, ctx:sqlParser.ExprAndContext):
        pass


    # Enter a parse tree produced by sqlParser#exprParenthesis.
    def enterExprParenthesis(self, ctx:sqlParser.ExprParenthesisContext):
        pass

    # Exit a parse tree produced by sqlParser#exprParenthesis.
    def exitExprParenthesis(self, ctx:sqlParser.ExprParenthesisContext):
        pass


    # Enter a parse tree produced by sqlParser#exprIsNot.
    def enterExprIsNot(self, ctx:sqlParser.ExprIsNotContext):
        pass

    # Exit a parse tree produced by sqlParser#exprIsNot.
    def exitExprIsNot(self, ctx:sqlParser.ExprIsNotContext):
        pass


    # Enter a parse tree produced by sqlParser#foreign_key_clause.
    def enterForeign_key_clause(self, ctx:sqlParser.Foreign_key_clauseContext):
        pass

    # Exit a parse tree produced by sqlParser#foreign_key_clause.
    def exitForeign_key_clause(self, ctx:sqlParser.Foreign_key_clauseContext):
        pass


    # Enter a parse tree produced by sqlParser#table_constraint.
    def enterTable_constraint(self, ctx:sqlParser.Table_constraintContext):
        pass

    # Exit a parse tree produced by sqlParser#table_constraint.
    def exitTable_constraint(self, ctx:sqlParser.Table_constraintContext):
        pass


    # Enter a parse tree produced by sqlParser#ordering_term.
    def enterOrdering_term(self, ctx:sqlParser.Ordering_termContext):
        pass

    # Exit a parse tree produced by sqlParser#ordering_term.
    def exitOrdering_term(self, ctx:sqlParser.Ordering_termContext):
        pass


    # Enter a parse tree produced by sqlParser#common_table_expression.
    def enterCommon_table_expression(self, ctx:sqlParser.Common_table_expressionContext):
        pass

    # Exit a parse tree produced by sqlParser#common_table_expression.
    def exitCommon_table_expression(self, ctx:sqlParser.Common_table_expressionContext):
        pass


    # Enter a parse tree produced by sqlParser#resultColumnAsterisk.
    def enterResultColumnAsterisk(self, ctx:sqlParser.ResultColumnAsteriskContext):
        pass

    # Exit a parse tree produced by sqlParser#resultColumnAsterisk.
    def exitResultColumnAsterisk(self, ctx:sqlParser.ResultColumnAsteriskContext):
        pass


    # Enter a parse tree produced by sqlParser#resultColumnTableAsterisk.
    def enterResultColumnTableAsterisk(self, ctx:sqlParser.ResultColumnTableAsteriskContext):
        pass

    # Exit a parse tree produced by sqlParser#resultColumnTableAsterisk.
    def exitResultColumnTableAsterisk(self, ctx:sqlParser.ResultColumnTableAsteriskContext):
        pass


    # Enter a parse tree produced by sqlParser#resultColumnExpr.
    def enterResultColumnExpr(self, ctx:sqlParser.ResultColumnExprContext):
        pass

    # Exit a parse tree produced by sqlParser#resultColumnExpr.
    def exitResultColumnExpr(self, ctx:sqlParser.ResultColumnExprContext):
        pass


    # Enter a parse tree produced by sqlParser#table_or_subquery.
    def enterTable_or_subquery(self, ctx:sqlParser.Table_or_subqueryContext):
        pass

    # Exit a parse tree produced by sqlParser#table_or_subquery.
    def exitTable_or_subquery(self, ctx:sqlParser.Table_or_subqueryContext):
        pass


    # Enter a parse tree produced by sqlParser#join_clause.
    def enterJoin_clause(self, ctx:sqlParser.Join_clauseContext):
        pass

    # Exit a parse tree produced by sqlParser#join_clause.
    def exitJoin_clause(self, ctx:sqlParser.Join_clauseContext):
        pass


    # Enter a parse tree produced by sqlParser#join_operator.
    def enterJoin_operator(self, ctx:sqlParser.Join_operatorContext):
        pass

    # Exit a parse tree produced by sqlParser#join_operator.
    def exitJoin_operator(self, ctx:sqlParser.Join_operatorContext):
        pass


    # Enter a parse tree produced by sqlParser#join_constraint.
    def enterJoin_constraint(self, ctx:sqlParser.Join_constraintContext):
        pass

    # Exit a parse tree produced by sqlParser#join_constraint.
    def exitJoin_constraint(self, ctx:sqlParser.Join_constraintContext):
        pass


    # Enter a parse tree produced by sqlParser#select_core.
    def enterSelect_core(self, ctx:sqlParser.Select_coreContext):
        pass

    # Exit a parse tree produced by sqlParser#select_core.
    def exitSelect_core(self, ctx:sqlParser.Select_coreContext):
        pass


    # Enter a parse tree produced by sqlParser#compound_operator.
    def enterCompound_operator(self, ctx:sqlParser.Compound_operatorContext):
        pass

    # Exit a parse tree produced by sqlParser#compound_operator.
    def exitCompound_operator(self, ctx:sqlParser.Compound_operatorContext):
        pass


    # Enter a parse tree produced by sqlParser#cte_table_name.
    def enterCte_table_name(self, ctx:sqlParser.Cte_table_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#cte_table_name.
    def exitCte_table_name(self, ctx:sqlParser.Cte_table_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#signed_number.
    def enterSigned_number(self, ctx:sqlParser.Signed_numberContext):
        pass

    # Exit a parse tree produced by sqlParser#signed_number.
    def exitSigned_number(self, ctx:sqlParser.Signed_numberContext):
        pass


    # Enter a parse tree produced by sqlParser#literal_value.
    def enterLiteral_value(self, ctx:sqlParser.Literal_valueContext):
        pass

    # Exit a parse tree produced by sqlParser#literal_value.
    def exitLiteral_value(self, ctx:sqlParser.Literal_valueContext):
        pass


    # Enter a parse tree produced by sqlParser#unary_operator.
    def enterUnary_operator(self, ctx:sqlParser.Unary_operatorContext):
        pass

    # Exit a parse tree produced by sqlParser#unary_operator.
    def exitUnary_operator(self, ctx:sqlParser.Unary_operatorContext):
        pass


    # Enter a parse tree produced by sqlParser#error_message.
    def enterError_message(self, ctx:sqlParser.Error_messageContext):
        pass

    # Exit a parse tree produced by sqlParser#error_message.
    def exitError_message(self, ctx:sqlParser.Error_messageContext):
        pass


    # Enter a parse tree produced by sqlParser#module_argument.
    def enterModule_argument(self, ctx:sqlParser.Module_argumentContext):
        pass

    # Exit a parse tree produced by sqlParser#module_argument.
    def exitModule_argument(self, ctx:sqlParser.Module_argumentContext):
        pass


    # Enter a parse tree produced by sqlParser#column_alias.
    def enterColumn_alias(self, ctx:sqlParser.Column_aliasContext):
        pass

    # Exit a parse tree produced by sqlParser#column_alias.
    def exitColumn_alias(self, ctx:sqlParser.Column_aliasContext):
        pass


    # Enter a parse tree produced by sqlParser#keyword.
    def enterKeyword(self, ctx:sqlParser.KeywordContext):
        pass

    # Exit a parse tree produced by sqlParser#keyword.
    def exitKeyword(self, ctx:sqlParser.KeywordContext):
        pass


    # Enter a parse tree produced by sqlParser#name.
    def enterName(self, ctx:sqlParser.NameContext):
        pass

    # Exit a parse tree produced by sqlParser#name.
    def exitName(self, ctx:sqlParser.NameContext):
        pass


    # Enter a parse tree produced by sqlParser#function_name.
    def enterFunction_name(self, ctx:sqlParser.Function_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#function_name.
    def exitFunction_name(self, ctx:sqlParser.Function_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#database_name.
    def enterDatabase_name(self, ctx:sqlParser.Database_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#database_name.
    def exitDatabase_name(self, ctx:sqlParser.Database_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#table_name.
    def enterTable_name(self, ctx:sqlParser.Table_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#table_name.
    def exitTable_name(self, ctx:sqlParser.Table_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#table_or_index_name.
    def enterTable_or_index_name(self, ctx:sqlParser.Table_or_index_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#table_or_index_name.
    def exitTable_or_index_name(self, ctx:sqlParser.Table_or_index_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#new_table_name.
    def enterNew_table_name(self, ctx:sqlParser.New_table_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#new_table_name.
    def exitNew_table_name(self, ctx:sqlParser.New_table_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#new_database_name.
    def enterNew_database_name(self, ctx:sqlParser.New_database_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#new_database_name.
    def exitNew_database_name(self, ctx:sqlParser.New_database_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#column_name.
    def enterColumn_name(self, ctx:sqlParser.Column_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#column_name.
    def exitColumn_name(self, ctx:sqlParser.Column_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#collation_name.
    def enterCollation_name(self, ctx:sqlParser.Collation_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#collation_name.
    def exitCollation_name(self, ctx:sqlParser.Collation_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#foreign_table.
    def enterForeign_table(self, ctx:sqlParser.Foreign_tableContext):
        pass

    # Exit a parse tree produced by sqlParser#foreign_table.
    def exitForeign_table(self, ctx:sqlParser.Foreign_tableContext):
        pass


    # Enter a parse tree produced by sqlParser#index_name.
    def enterIndex_name(self, ctx:sqlParser.Index_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#index_name.
    def exitIndex_name(self, ctx:sqlParser.Index_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#trigger_name.
    def enterTrigger_name(self, ctx:sqlParser.Trigger_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#trigger_name.
    def exitTrigger_name(self, ctx:sqlParser.Trigger_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#view_name.
    def enterView_name(self, ctx:sqlParser.View_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#view_name.
    def exitView_name(self, ctx:sqlParser.View_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#module_name.
    def enterModule_name(self, ctx:sqlParser.Module_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#module_name.
    def exitModule_name(self, ctx:sqlParser.Module_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#table_alias.
    def enterTable_alias(self, ctx:sqlParser.Table_aliasContext):
        pass

    # Exit a parse tree produced by sqlParser#table_alias.
    def exitTable_alias(self, ctx:sqlParser.Table_aliasContext):
        pass


    # Enter a parse tree produced by sqlParser#transaction_name.
    def enterTransaction_name(self, ctx:sqlParser.Transaction_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#transaction_name.
    def exitTransaction_name(self, ctx:sqlParser.Transaction_nameContext):
        pass


    # Enter a parse tree produced by sqlParser#any_name.
    def enterAny_name(self, ctx:sqlParser.Any_nameContext):
        pass

    # Exit a parse tree produced by sqlParser#any_name.
    def exitAny_name(self, ctx:sqlParser.Any_nameContext):
        pass


