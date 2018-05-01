import sys
import os
import json
import ast
#para crear bases de datos  estoy usando pathlib ya no os
import pathlib
#para cualquier definicion de funcion que usemos
import extra
import shutil
from antlr4 import *
from sqlLexer import sqlLexer
from sqlParser import sqlParser
from sqlListener import sqlListener
from antlr4.error.ErrorListener import ErrorListener


userpath = 'databases/'
db = ""

tiposDatos = ['INT','FLOAT', 'DATE', 'CHAR']

class GeneralListener(sqlListener):

        #Operaciones con BASES DE DATOS   ----------------------------------------------------------------------------------------------

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
        # print([x[0].replace(userpath, "") for x in os.walk(userpath)])
        temuserpath = userpath[:-1]
        print([x[0].replace(temuserpath, "") for x in extra.walklevel(temuserpath, 1)])

    # Exit a parse tree produced by sqlParser#use_database_stmt.
    def exitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        global db
        existe = pathlib.Path(userpath + ctx.database_name().getText()).exists()
        if existe:
            db = ctx.database_name().getText()
            print("Ahora esta usando la base de datos " + db)
        else:
            print("No existe la base de datos!")

    def exitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        print("¿Esta seguro de que quiere eliminar la base de datos: " +ctx.database_name().getText() + "  con N Registros?")
        print("Y/N para proceder")
        respuesta = input()
        if (respuesta == "Y" or respuesta == "y"):
            print("Se elimino la base de datos: " + ctx.database_name().getText())
            shutil.rmtree(userpath + ctx.database_name().getText(), ignore_errors=True)
        else:
            print("No se elimino ninguna base de datos")


            #Operaciones con TABLAS------------------------------------------------------------------------------------------------------

    # Exit a parse tree produced by sqlParser#create_table_stmt.
    def exitCreate_table_stmt(self, ctx:sqlParser.Create_table_stmtContext):
        global db
        contador = 0

        if db != "":
            tableName = ctx.table_name().getText()
            #Buscar si ya existe
            existe = pathlib.Path(userpath + "/" + db +"/"+ tableName).exists()
            if existe:
                print("Ya existe esta tabla")
            else:
                #todo bien o mal
                error = True

                #crear folder
                pathlib.Path(userpath + "/" + db +"/"+ tableName).mkdir(parents=True, exist_ok=True)
                #crear archivos
                direccion = userpath + "/" + db +"/"+ tableName         #direccion
                schema = open(direccion + "/schema.json", "w")          #crear schema
                data = open(direccion + "/data.txt", "w")               #crear data
                #llenar schema
                schemaData = {}
                data = []
                #ctx.table_constraint()[0].column_name()[0].getText() + "" +
                #print(type(key))
                for column in ctx.column_def():
                    schemaColumn = {}
                    schemaColumn['nombre'] = column.column_name().getText()
                    schemaColumn['type'] = column.type_name().getText()
                    if schemaColumn['type'] in tiposDatos:
                        print ("El tipo de dato ingresado es valido")
                    else:
                        contador = contador + 1
                    schemaColumn['key'] = ''                   #No se como sacar la key
                    data.append(schemaColumn)

                for constraint in ctx.table_constraint():
                    for d in data:
                        if d['nombre'] == constraint.column_name()[0].getText():
                            if constraint.K_PRIMARY() != None:
                                print('constraint es primary')
                                d['key'] = 'primary'
                            elif constraint.K_FOREIGN() != None:
                                print('constraint es foreign')
                                referencia = {}
                                referencia['tabla'] = constraint.foreign_key_clause().foreign_table().getText()
                                referencia['columna'] = constraint.foreign_key_clause().column_name()[0].getText()
                                #checkear si existe tabla y columna referenciada
                                print('Checkeando referencia')
                                tableRef = userpath + '/' + db + '/' + referencia['tabla']
                                if pathlib.Path(tableRef).exists():
                                    print('Si existe tabla')
                                    #checkear la existencia de la columna
                                    jsonRef = open(tableRef + '/schema.json', 'r')
                                    refRead = jsonRef.read()
                                    json = ast.literal_eval(refRead)
                                    its_in = 0
                                    for dato in json['data']:
                                        if dato['nombre'] == referencia['columna']:
                                            its_in = its_in + 1
                                    if its_in > 0:
                                        print('existe columna referenciada')
                                        d['key'] = 'foreign'
                                        d['referencia'] = referencia
                                        print(d)
                                        print(data)
                                    else:
                                        print('no existe columna')
                                        contador = contador + 1
                                else:
                                    contador = contador + 1
                            else:
                                d['key'] = ''

                if contador > 0:
                    print ("Borrando la tabla")
                    shutil.rmtree(direccion, ignore_errors=True)
                else:
                    schemaData['registros'] = '0'
                    schemaData["data"] = data
                    schema.write(str(schemaData))

        else:
            print("No hay ninguna base de datos seleccionada")

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

    def exitDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        global db
        if db != "":
            print("¿Esta seguro de que quiere eliminar la tabla: " +ctx.table_name().getText() + " de la base de datos " + db)
            print("Y/N para proceder")
            respuesta = input()
            if (respuesta == "Y" or respuesta == "y"):
                print("Se elimino tabla: " + ctx.table_name().getText())
                shutil.rmtree(userpath + "/" + db + "/" + ctx.table_name().getText(), ignore_errors=True)
            else:
                print("No se elimino ninguna tabla")
        else:
            print("No hay ninguna base de datos seleccionada")

    def exitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        global db
        if db != "":
            tmppath = userpath + db
            print("Las tablas de la base de datos: " + db + " son: ")
            print([x[0].replace(tmppath, "") for x in extra.walklevel(tmppath, 1)])
        else:
            print("No hay ninguna base de datos seleccionada")

    # Exit a parse tree produced by sqlParser#insert_stmt.
    def exitInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        global db
        if db != "":
            tableName = ctx.table_name().getText()
            direccion = userpath + "/" + db +"/"+ tableName
            tableColumns = ctx.column_name()
            tableValues = ctx.expr()

            schema = open(direccion + "/schema.json", "r")
            text = schema.read()
            schema.close()
            json = ast.literal_eval(text)
            schemaColumns = ast.literal_eval(text)['data']
            if len(tableColumns) == len(tableValues) and len(tableValues) == len(schemaColumns):
                #tienen la misma cantidad de valores
                print ("La cantidad de valores ingresados coincide con el numero de tuplas de la tabla")
                dato = ""
                c = 0
                for data in tableValues:
                    if extra.type_anything(data.getText()) != schemaColumns[c]['type']:
                        print("El tipo esperado: " + schemaColumns[c]['type'] + ", se encontro: " + extra.type_anything(data.getText()))
                        return 1
                    dato = dato + data.getText() + "|"
                    c = c + 1
                dato = dato[:-1]
                #escribir data a data.txt
                dataFile = open(direccion + "/data.txt", "a")
                dataFile.write(dato + '\n')
                dataFile.close()
                #cambiar registro en json
                json['registros'] = ''+ str(int(json['registros']) + 1) + ''
                schemaJSON = open(direccion + "/schema.json", "w")
                schemaJSON.write(str(json))
                schemaJSON.close()


                #recorrer todos los valor y guardarlos en el archivo
            else:
                #no tienen la misma cantidad de valores
                print ("La cantidad de valores ingresados NO coincide con el numero de tuplas de la tabla")

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

            parse(text)
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
