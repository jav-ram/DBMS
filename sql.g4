/*
 * Gramática simplificada basada en SQLite para propósitos del curso de bases de
 * datos UVG CC3057.
 *
 * Retreived from: https://raw.githubusercontent.com/antlr/grammars-v4/master/sqlite/SQLite.g4
 */
grammar sql;

/**
 * Parser rules
 */
parse
 : ( sql_stmt_list | error )* EOF
 ;

error
 : UNEXPECTED_CHAR
  {raise Exception("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text)}
 ;

sql_stmt_list
 : ';'* sql_stmt ( ';'+ sql_stmt )* ';'*
 ;

sql_stmt
 : ( create_database_stmt
      | alter_database_stmt
      | drop_database_stmt
      | show_databases_stmt
      | show_tables_stmt
      | use_database_stmt
      | create_table_stmt
      | alter_table_stmt
      | show_columns_stmt
      | begin_stmt
      | commit_stmt
      | create_index_stmt
      | delete_stmt
      | drop_index_stmt
      | drop_table_stmt
      | factored_select_stmt
      | insert_stmt
      | rollback_stmt
      | simple_select_stmt
      | update_stmt )
 ;

create_database_stmt
 : K_CREATE K_DATABASE database_name
 ;

alter_database_stmt
 : K_ALTER K_DATABASE database_name K_RENAME K_TO new_database_name
 ;

drop_database_stmt
 : K_DROP K_DATABASE database_name
 ;

show_databases_stmt
 : K_SHOW K_DATABASES
 ;

show_tables_stmt
 : K_SHOW K_TABLES
 ;

use_database_stmt
 : K_USE K_DATABASE database_name
 ;

alter_table_stmt
 : K_ALTER K_TABLE table_name
   ( K_RENAME K_TO new_table_name
    | K_ADD K_COLUMN column_def
    | K_ADD table_constraint
    | K_DROP K_COLUMN column_name
    | K_DROP K_CONSTRAINT name
   )
 ;

show_columns_stmt
 : K_SHOW K_COLUMNS K_FROM table_name
 ;

begin_stmt
 : K_BEGIN ( K_TRANSACTION transaction_name? )?
 ;

commit_stmt
 : K_COMMIT ( K_TRANSACTION transaction_name? )?
 ;

/**
 * Esto agrupa posibles SELECT statements unidos por operaciones de conjuntos: 
 * UNION, INTERSECT, EXCEPT
 */
factored_select_stmt
 : select_core ( compound_operator select_core )*
   ( K_ORDER K_BY ordering_term ( ',' ordering_term )* )?
   ( K_LIMIT expr ( ( K_OFFSET | ',' ) expr )? )?
 ;

create_index_stmt
 : K_CREATE K_INDEX
   index_name K_ON table_name '(' column_name ( ',' column_name )* ')'
 ;

create_table_stmt
 : K_CREATE K_TABLE
   table_name
   '(' column_def ( ',' column_def )* ( ',' table_constraint )* ')'
 ;

delete_stmt
 : K_DELETE K_FROM table_name
   ( K_WHERE expr )?
 ;

drop_index_stmt
 : K_DROP K_INDEX index_name
 ;

drop_table_stmt
 : K_DROP K_TABLE table_name
 ;

insert_stmt
 : K_INSERT K_INTO
   table_name ( '(' column_name ( ',' column_name )* ')' )?
   K_VALUES '(' expr ( ',' expr )* ')'
 ;

rollback_stmt
 : K_ROLLBACK
 ;

simple_select_stmt
 : select_core ( K_ORDER K_BY ordering_term ( ',' ordering_term )* )?
   ( K_LIMIT expr ( ( K_OFFSET | ',' ) expr )? )?
 ;

update_stmt
 : K_UPDATE table_name
   K_SET column_name '=' expr ( ',' column_name '=' expr )* ( K_WHERE expr )?
 ;

column_def
 : column_name type_name column_constraint*
 ;

type_name
 : name+ ( '(' signed_number ')'
         | '(' signed_number ',' signed_number ')' )?
 ;

column_constraint
 : K_DEFAULT (signed_number | literal_value | '(' expr ')')
 ;

/*
    SQLite understands the following binary operators, in order from highest to
    lowest precedence:

    *    /    %
    +    -
    <<   >>   &    |
    <    <=   >    >=
    =    ==   !=   <>   IS   IS NOT   IN   LIKE   GLOB   MATCH
    AND
    OR
    NOT
*/
expr
 : literal_value                                                                # exprLiteralValue
 | ( table_name '.' )? column_name                                              # exprTableColumn
 | unary_operator expr                                                          # exprUnaryExpr
 | expr ( '*' | '/' | '%' ) expr                                                # exprMul
 | expr ( '+' | '-' ) expr                                                      # exprAdd
 | expr ( '<' | '<=' | '>' | '>=' ) expr                                        # exprComparisonFirst
 | expr ( '=' | '==' | '!=' | '<>' | K_IS | K_IS K_NOT | K_IN | K_LIKE | K_MATCH ) expr     # exprComparisonSecond
 | expr K_AND expr                                                              # exprAnd
 | expr K_OR expr                                                               # exprOr
 | function_name '(' ( K_DISTINCT? expr ( ',' expr )* | '*' )? ')'              # exprFunction
 | K_NOT expr                                                                   # exprNot
 | '(' expr ')'                                                                 # exprParenthesis
 | expr K_NOT? ( K_LIKE ) expr                                                  # exprLike
 | expr ( K_NOT K_NULL )                                                        # exprNotNull
 | expr K_IS K_NOT? expr                                                        # exprIsNot
 | expr K_NOT? K_IN ( '(' ( select_core | expr ( ',' expr )* )? ')' | table_name ) # exprNotIn
 | ( ( K_NOT )? K_EXISTS )? '(' select_core ')'                                 # exprNotExists
 ;

foreign_key_clause
 : K_REFERENCES foreign_table ( '(' column_name ( ',' column_name )* ')' )?
 ;

table_constraint
 : K_CONSTRAINT name
   ( ( K_PRIMARY K_KEY | K_UNIQUE ) '(' column_name ( ',' column_name )* ')'
   | K_CHECK '(' expr ')'
   | K_FOREIGN K_KEY '(' column_name ( ',' column_name )* ')' foreign_key_clause
   )
 ;

ordering_term
 : expr ( K_ASC | K_DESC )?
 ;

common_table_expression
 : table_name ( '(' column_name ( ',' column_name )* ')' )? K_AS '(' select_core ')'
 ;

result_column
 : '*'                              # resultColumnAsterisk
 | table_name '.' '*'               # resultColumnTableAsterisk
 | expr ( K_AS? column_alias )?     # resultColumnExpr
 ;

table_or_subquery
 : table_name ( K_AS? table_alias )?
 | '(' ( table_or_subquery ( ',' table_or_subquery )*
       | join_clause )
   ')' ( K_AS? table_alias )?
 | '(' select_core ')' ( K_AS? table_alias )?
 ;

join_clause
 : table_or_subquery ( join_operator table_or_subquery join_constraint )*
 ;

join_operator
 : ','
 | ( K_LEFT | K_INNER ) K_JOIN
 ;

join_constraint
 : ( K_ON expr )?
 ;

select_core
 : K_SELECT ( K_DISTINCT )? result_column ( ',' result_column )*
   ( K_FROM ( table_or_subquery ( ',' table_or_subquery )* | join_clause ) )?
   ( K_WHERE expr )?
   ( K_GROUP K_BY expr ( ',' expr )* ( K_HAVING expr )? )?
 ;

compound_operator
 : K_UNION
 | K_INTERSECT
 | K_EXCEPT
 ;

cte_table_name
 : table_name ( '(' column_name ( ',' column_name )* ')' )?
 ;

signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | BLOB_LITERAL
 | K_NULL
 ;

unary_operator
 : '-'
 | '+'
 | '~'
 | K_NOT
 ;

error_message
 : STRING_LITERAL
 ;

module_argument // TODO check what exactly is permitted here
 : expr
 | column_def
 ;

column_alias
 : IDENTIFIER
 | STRING_LITERAL
 ;

keyword
 : K_ADD
 | K_ALTER
 | K_AND
 | K_AS
 | K_ASC
 | K_BEGIN
 | K_BY
 | K_CHECK
 | K_COLUMN
 | K_COMMIT
 | K_CONSTRAINT
 | K_CREATE
 | K_DATABASE
 | K_DATABASES
 | K_DEFAULT
 | K_DELETE
 | K_DESC
 | K_DISTINCT
 | K_DROP
 | K_EXCEPT
 | K_EXISTS
 | K_FOR
 | K_FOREIGN
 | K_FROM
 | K_FULL
 | K_GROUP
 | K_HAVING
 | K_IGNORE
 | K_IN
 | K_INDEX
 | K_INNER
 | K_INSERT
 | K_INTERSECT
 | K_INTO
 | K_IS
 | K_JOIN
 | K_KEY
 | K_LEFT
 | K_LIKE
 | K_LIMIT
 | K_NOT
 | K_NULL
 | K_OF
 | K_OFFSET
 | K_ON
 | K_OR
 | K_ORDER
 | K_PRIMARY
 | K_REFERENCES
 | K_RENAME
 | K_ROLLBACK
 | K_SELECT
 | K_SET
 | K_SHOW
 | K_TABLE
 | K_TABLES
 | K_THEN
 | K_TO
 | K_TRANSACTION
 | K_USE
 | K_UNION
 | K_UNIQUE
 | K_UPDATE
 | K_VALUES
 | K_WHERE
 ;

// TODO check all names below

name
 : any_name
 ;

function_name
 : any_name
 ;

database_name
 : any_name
 ;

table_name
 : any_name
 ;

table_or_index_name
 : any_name
 ;

new_table_name
 : any_name
 ;

new_database_name
 : any_name
 ;

column_name
 : any_name
 ;

collation_name
 : any_name
 ;

foreign_table
 : any_name
 ;

index_name
 : any_name
 ;

trigger_name
 : any_name
 ;

view_name
 : any_name
 ;

module_name
 : any_name
 ;

table_alias
 : any_name
 ;

transaction_name
 : any_name
 ;

any_name
 : IDENTIFIER
 | keyword
 | STRING_LITERAL
 | '(' any_name ')'
 ;

/**
 * Lexer rules
 *
 * http://www.sqlite.org/lang_keywords.html
 */
K_ADD : A D D;
K_ALTER : A L T E R;
K_AND : A N D;
K_AS : A S;
K_ASC : A S C;
K_BEGIN : B E G I N;
K_BY : B Y;
K_CHECK : C H E C K;
K_COLUMN : C O L U M N;
K_COLUMNS : C O L U M N S;
K_COMMIT : C O M M I T;
K_CONSTRAINT : C O N S T R A I N T;
K_CREATE : C R E A T E;
K_DATABASE : D A T A B A S E;
K_DATABASES : D A T A B A S E S;
K_DEFAULT: D E F A U L T;
K_DELETE : D E L E T E;
K_DESC : D E S C;
K_DISTINCT : D I S T I N C T;
K_DROP : D R O P;
K_EXCEPT : E X C E P T;
K_EXISTS : E X I S T S;
K_FOR : F O R;
K_FOREIGN : F O R E I G N;
K_FROM : F R O M;
K_FULL : F U L L;
K_GROUP : G R O U P;
K_HAVING : H A V I N G;
K_IGNORE : I G N O R E;
K_IN : I N;
K_INDEX : I N D E X;
K_INNER : I N N E R;
K_INSERT : I N S E R T;
K_INTERSECT : I N T E R S E C T;
K_INTO : I N T O;
K_IS : I S;
K_JOIN : J O I N;
K_KEY : K E Y;
K_LEFT : L E F T;
K_LIKE : L I K E;
K_LIMIT : L I M I T;
K_MATCH : M A T C H;
K_NATURAL : N A T U R A L;
K_NO : N O;
K_NOT : N O T;
K_NULL : N U L L;
K_OF : O F;
K_OFFSET : O F F S E T;
K_ON : O N;
K_OR : O R;
K_ORDER : O R D E R;
K_PRIMARY : P R I M A R Y;
K_REFERENCES : R E F E R E N C E S;
K_RENAME : R E N A M E;
K_ROLLBACK : R O L L B A C K;
K_SELECT : S E L E C T;
K_SET : S E T;
K_SHOW : S H O W;
K_TABLE : T A B L E;
K_TABLES : T A B L E S;
K_THEN : T H E N;
K_TO : T O;
K_TRANSACTION : T R A N S A C T I O N;
K_UNION : U N I O N;
K_UNIQUE : U N I Q U E;
K_UPDATE : U P D A T E;
K_USE : U S E;
K_VALUES : V A L U E S;
K_WHERE : W H E R E;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | '`' (~'`' | '``')* '`'
 | '[' ~']'* ']'
 | [a-zA-Z_] [a-zA-Z_0-9]* // TODO check: needs more chars in set
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( E [-+]? DIGIT+ )?
 | '.' DIGIT+ ( E [-+]? DIGIT+ )?
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

BLOB_LITERAL
 : X STRING_LITERAL
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];
