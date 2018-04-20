// Definir una gramática llamada Hello
grammar Hello;

// Lexer rules: estas no generan puntos de acceso en el listener
ID : [a-z]+ ;             // match lower-case identifiers
WS : [ \t\r\n]+ -> skip ; // skip spaces, tabs, newlines

// Parser rules: estas generan puntos de acceso en el listener
r  : 'hello' ID ;         // match keyword hello followed by an identifier
