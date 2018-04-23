import sys
import os
#para crear bases de datos  estoy usando pathlib ya no os
import pathlib
import shutil
from antlr4 import *
from sqlLexer import sqlLexer
from sqlParser import sqlParser
from sqlListener import sqlListener
from antlr4.error.ErrorListener import ErrorListener


userpath = '/databases/'
db = ""

class GeneralListener(sqlListener):
    def exitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        # os.mkdir(ctx.database_name().getText())
        pathlib.Path(userpath + ctx.database_name().getText()).mkdir(parents=True, exist_ok=True)
        print("Base de datos: " + ctx.database_name().getText() + " Creada!")
        #   print("Hello world. At the input has been already validated")

    def exitAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        old_name = pathlib.Path(userpath + ctx.database_name().getText())

        print("¿Esta seguro que quiere renombrar la base de datos: " + ctx.database_name().getText() + " por " + ctx.new_database_name().getText() + "?")
        print("Y/N para proceder")
        respuesta = input()
        if (respuesta == "Y" or respuesta == "y"):
            print("El nombre de la base de datos: " + ctx.database_name().getText() + "ha cambiado a: " + ctx.new_database_name().getText())
            old_name.rename(userpath + ctx.new_database_name().getText())
        else:
            print("No se cambio el nombre de la base de datos")

    def exitShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        # os.walk(userpath)
        print([x[0].replace(userpath, "") for x in os.walk(userpath)])

    # Exit a parse tree produced by sqlParser#use_database_stmt.
    def exitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        global db
        existe = pathlib.Path(userpath + ctx.database_name().getText()).exists()
        if existe:
            db = ctx.database_name().getText()
            print("Ahora esta usando la base de datos " + db)
        else:
            print("No existe la base de datos!")

    # Exit a parse tree produced by sqlParser#create_table_stmt.
    def exitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        global db
        if db != "":
            tableName = ctx.table_name().getText()
            #Buscar si ya existe
            existe = pathlib.Path(userpath + "/" + db +"/"+ tableName).exists()
            if existe:
                print("Ya existe esta tabla")
            else:
                pathlib.Path(userpath + "/" + db +"/"+ tableName).mkdir(parents=True, exist_ok=True)
                print("Se creo nueva tabla " + tableName)

        else:
            print("No hay ninguna base de datos seleccionada")

    def exitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        print("¿Esta seguro de que quiere eliminar la base de datos: " +ctx.database_name().getText() + "  con N Registros?")
        print("Y/N para proceder")
        respuesta = input()
        if (respuesta == "Y" or respuesta == "y"):
            print("Se elimino la base de datos: " + ctx.database_name().getText())
            shutil.rmtree(userpath + ctx.database_name().getText(), ignore_errors=True)
        else:
            print("No se elimino ninguna base de datos")

    def exitAlter_table_stmt(self, ctx:sqlParser.Alter_table_stmtContext):
        global db
        if db != "":
            tableName = ctx.table_name().getText()
            existe = pathlib.Path(userpath + "/" + db +"/"+ tableName).exists()
            if existe:
                old_name = pathlib.Path(userpath + "/" + db + "/" + tableName)
                old_name.rename(userpath + "/" + db + "/"+ ctx.new_table_name().getText())

            else:
                print("No existe la tabla que se quiere renombrar")

        else:
            print("No hay ninguna base de datos seleccionada")

class ParserException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParserExceptionErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise ParserException("line " + str(line) + ":" + str(column) + " " + msg)

def parse(text):
    lexer = sqlLexer(InputStream(text))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParserExceptionErrorListener())

    stream = CommonTokenStream(lexer)

    parser = sqlParser(stream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParserExceptionErrorListener())

    # Este es el nombre de la produccion inicial de la gramatica definida en sql.g4
    tree = parser.parse()

    testListener = GeneralListener()
    walker = ParseTreeWalker()
    walker.walk(testListener, tree)

'''
Uso: python cli.py

Las construcciones validas para esta gramatica son todas aquellas
'''
def main(argv):
    while True:
        try:
            text = input("> ")

            if (text == 'exit'):
                sys.exit()

            parse(text);
            print("Valid")

        except ParserException as e:
            print("Got a parser exception:", e.value)

        except EOFError as e:
            print("Bye")
            sys.exit()

        except Exception as e:
            print("Got exception: ", e)

if __name__ == '__main__':
    main(sys.argv)
