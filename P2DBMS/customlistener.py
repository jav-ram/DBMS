##################################################################################################################################################
# Universidad del Valle de Guatemala                                                                                                             #
# Bases de Datos                                                                                                                                 #
# Autores: Javier Ramos (16), José Rodolfo Pérez (16056), Andrea Cordon (16076)                                                                  #
##################################################################################################################################################

# Se importan todas las librerias a utilizar
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

# Se define la direccion de donde se estaran manejando las Bases de Datos
userpath = 'databases/'
db = ""

# Se definen los tipos de datos que seran permitidos en el DBMS
tiposDatos = ['INT','FLOAT', 'DATE', 'CHAR']

# Se crea la clase general que contendra todos los metodos del DBMS
class GeneralListener(sqlListener):

            ############################################################################################################################################################
            #                                                                                                                                                          #
            #                                                           Operaciones con BASES DE DATOS                                                                 #
            #                                                                                                                                                          #
            ############################################################################################################################################################

    # CREATE TABLE
    def exitCreate_database_stmt(self, ctx:sqlParser.Create_database_stmtContext):
        #Crear un directorio para la base de datos
        pathlib.Path(userpath + ctx.database_name().getText()).mkdir(parents=True, exist_ok=True)
        print("Base de datos: " + ctx.database_name().getText() + " Creada!")

    # ALTER DATABASE
    def exitAlter_database_stmt(self, ctx:sqlParser.Alter_database_stmtContext):
        #Nombre viejo de la base de datos que se desea renombrar
        old_name = pathlib.Path(userpath + ctx.database_name().getText())
        #verificacion de seguridad
        # Se le pregunta al usuario si realmente quiere cambiar el nombre a la base de datos
        print("¿Esta seguro que quiere renombrar la base de datos: " + ctx.database_name().getText() + " por " + ctx.new_database_name().getText() + "?")
        print("Y/N para proceder")
        respuesta = input()
        # Si la respuesta es si
        if (respuesta == "Y" or respuesta == "y"):
            print("El nombre de la base de datos: " + ctx.database_name().getText() + "ha cambiado a: " + ctx.new_database_name().getText())
            #renombrar la carpeta de la DB
            old_name.rename(userpath + ctx.new_database_name().getText())
        # Si no se desea cambiar la base de datos
        else:
            print("No se cambio el nombre de la base de datos")

    # SHOW DATABASES
    def exitShow_databases_stmt(self, ctx:sqlParser.Show_databases_stmtContext):
        #Eliminar la barra "/"
        temuserpath = userpath[:-1]
        #Caminar el directorio solamente el primer nivel donde se encuentran las carpetas de bases de datos
        print([x[0].replace(temuserpath, "") for x in extra.walklevel(temuserpath, 1)])

    # USE DATABASE
    def exitUse_database_stmt(self, ctx:sqlParser.Use_database_stmtContext):
        global db
        #verificacion de que si existe la DB
        existe = pathlib.Path(userpath + ctx.database_name().getText()).exists()
        if existe:
            #cambiar la global con la DB utilizada
            db = ctx.database_name().getText()
            print("Ahora esta usando la base de datos " + db)
        else:
            print("No existe la base de datos!")

    # DROP DATABASE
    def exitDrop_database_stmt(self, ctx:sqlParser.Drop_database_stmtContext):
        #verificacion de drop statement de toda la DB
        print("¿Esta seguro de que quiere eliminar la base de datos: " +ctx.database_name().getText() + "  con N Registros?")
        print("Y/N para proceder")
        respuesta = input()
        if (respuesta == "Y" or respuesta == "y"):
            print("Se elimino la base de datos: " + ctx.database_name().getText())
            #Eliminar la carpeta y todas sus subcarpetas
            shutil.rmtree(userpath + ctx.database_name().getText(), ignore_errors=True)
        else:
            print("No se elimino ninguna base de datos")

    ############################################################################################################################################################
    #                                                                                                                                                          #
    #                                                                 Operaciones con TABLAS                                                                   #
    #                                                                                                                                                          #
    ############################################################################################################################################################

    # CREATE TABLE
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

                for column in ctx.column_def():
                    schemaColumn = {}
                    schemaColumn['nombre'] = column.column_name().getText()
                    schemaColumn['type'] = column.type_name().getText()
                    if schemaColumn['type'] in tiposDatos:
                        print ("El tipo de dato ingresado es valido")
                    else:
                        contador = contador + 1
                    schemaColumn['key'] = ''
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
                                    #Revisa la existencia de la columna
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
        #verificar si existe la DB
        if db != "":
            tableName = ctx.table_name().getText()
            existe = pathlib.Path(userpath + "/" + db +"/"+ tableName).exists()
            if existe:
                old_name = pathlib.Path(userpath + "/" + db + "/" + tableName)
                #renombrar la tabla (archivos)
                old_name.rename(userpath + "/" + db + "/"+ ctx.new_table_name().getText())

            else:
                print("No existe la tabla que se quiere renombrar")

        else:
            print("No hay ninguna base de datos seleccionada")

    # DROP TABLE
    def exitDrop_table_stmt(self, ctx:sqlParser.Drop_table_stmtContext):
        global db
        #verificar si existe la DB
        if db != "":
            #verificar si se desea realmente eliminar la tabla
            print("¿Esta seguro de que quiere eliminar la tabla: " +ctx.table_name().getText() + " de la base de datos " + db)
            print("Y/N para proceder")
            respuesta = input()
            if (respuesta == "Y" or respuesta == "y"):
                print("Se elimino tabla: " + ctx.table_name().getText())
                #eliminar la tabla
                shutil.rmtree(userpath + "/" + db + "/" + ctx.table_name().getText(), ignore_errors=True)
            else:
                print("No se elimino ninguna tabla")
        else:
            print("No hay ninguna base de datos seleccionada")

    # SHOW TABLES
    def exitShow_tables_stmt(self, ctx:sqlParser.Show_tables_stmtContext):
        global db
        #verificar si existe la DB
        if db != "":
            tmppath = userpath + db
            print("Las tablas de la base de datos: " + db + " son: ")
            #caminar el directorio de la base de datos para nombrar todas las tablas que se encuentran dentro
            print([x[0].replace(tmppath, "") for x in extra.walklevel(tmppath, 1)])
        else:
            print("No hay ninguna base de datos seleccionada")

    # Exit a parse tree produced by sqlParser#insert_stmt.
    # INSERT INTO TABLE VALUES
    def exitInsert_stmt(self, ctx:sqlParser.Insert_stmtContext):
        global db
        #verificar si existe la DB
        if db != "":
            tableName = ctx.table_name().getText()
            direccion = userpath + "/" + db +"/"+ tableName
            tableColumns = ctx.column_name()
            tableValues = ctx.expr()

            # Se abre el archivo schema.json para su lectura
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

                #SPAGHET DEL FUTURO PARA EL ULTIMO | (sino da problemas el split by)
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

#Update table con condiciones
    # UPDATE TABLE
    def exitUpdate_stmt(self, ctx:sqlParser.Update_stmtContext):
        global db
        if db != "":
            tableName = ctx.table_name().getText()
            direccion = userpath + "/" + db +"/"+ tableName
            tableColumn = ctx.column_name()[0].getText()
            tableValue = ctx.expr()[0].getText()
            schema = open(direccion + "/schema.json", "r")
            text = schema.read()
            json = ast.literal_eval(text)
            num_columns = len(json['data'])
            num_rows = json['registros']
            schema.close()
            jsonColumn = json['data']
            numerocolumna = 0

            #Obtener el numero de columnas en la base de datos
            for i in range (0, len(jsonColumn)):
                #print(jsonColumn[i]['nombre'])
                if (tableColumn == jsonColumn[i]['nombre']):
                    numerocolumna = i


            #Obtener los datos de la base de datos
            dataFile = open(direccion + "/data.txt", "r")
            datatext = str(dataFile.read())
            dataFile.close()

            #Separar los datos de la base de datos por enter (cada objeto ingresado a la DB)
            dataarray = datatext.split("\n")
            #Hacer un array bidimensional para cada atributo de la DB
            estructura = [[0 for x in range(int(num_columns))] for y in range(int(num_rows))]
            #Llenar el array bidimensional con los datos de la DB
            for y in range(0,len(dataarray)):
                columns = dataarray[y].split('|')
                for x in range(0,len(columns)-1):
                    estructura[y][x]= str(columns[x])



            try:
                #parse condition_raw
                conditionRaw = ctx.expr()[1].getText()
                newcondition = conditionRaw.replace(tableColumn, "")
                condicional = newcondition[:1]
                valor_condicional = newcondition[1:]

                print(condicional)
                print(valor_condicional)

                print(len(dataarray))
                #Obtener los datos que cumplen con el nombre de la tupla ingresada
                for f in range(0, len(dataarray)-2):
                    print(f)
                    #print(extra.where(estructura[f][numerocolumna], condicional, valor_condicional))
                    if extra.where(estructura[f][numerocolumna], condicional, valor_condicional):
                        print("adios")
                        #estructura[f][numerocolumna] = str(tableValue)
                print("hola")

                #crear un nuevo string para ingresar de nuevo a la base de datos luego de haberla operado
                nuevostr = ""
                #Lenar el string con los nuevos valores de el array bidimensional
                for k in range(0,int(num_rows)):
                    for t in range(0,int(num_columns)):
                        nuevostr = "" + nuevostr + str(estructura[k][t]) + "|"
                    nuevostr = nuevostr + "\n"

                #Impresion del array Bidimensional
                print(estructura)
                #Impresion del string ingresado a la DB
                print(nuevostr)

                #droppear la tabla
                newFile = open(direccion + "/data.txt", "w")
                #escribir en el archivo el nuevo string
                newFile.write(nuevostr)
                newFile.close()
            except:
                print("No tiene condicional, Por lo tanto se opero en toda la columna")
                #Obtener los datos que cumplen con el nombre de la tupla ingresada
                for f in range(0, len(dataarray)-1):
                    estructura[f][numerocolumna] = tableValue
                    #print(estructura[f][numerocolumna])

                #crear un nuevo string para ingresar de nuevo a la base de datos luego de haberla operado
                nuevostr = ""
                #Lenar el string con los nuevos valores de el array bidimensional
                for k in range(0,int(num_rows)):
                    for t in range(0,int(num_columns)):
                        nuevostr = "" + nuevostr + str(estructura[k][t]) + "|"
                    nuevostr = nuevostr + "\n"

                #Impresion del array Bidimensional
                print(estructura)
                #Impresion del string ingresado a la DB
                print(nuevostr)

                #droppear la tabla
                newFile = open(direccion + "/data.txt", "w")
                #escribir en el archivo el nuevo string
                newFile.write(nuevostr)
                newFile.close()

        else:
            # Si no se ha seleccionado ninguna base de datos
            print("Ninguna base de datos seleccionada")

    # SELECT
    def exitSelect_core(self, ctx:sqlParser.Select_coreContext):
        #sacar las columnas que se quieren
        columnas = []
        for i in range (0, len(ctx.result_column())):
            columnas.append(ctx.result_column()[i].getText())

        #sacar la tabla sobre cual es el select
        t = ctx.table_or_subquery()
        respuesta = []
        for item in t:
            tabla = item.getText()
            direccion = userpath + "/" + db +"/"+ tabla
            #verificar si existe columna y tabla
            #tabla
            existe = pathlib.Path(userpath + "/" + db +"/"+ tabla).exists()
            # Si la tabla mencionada existe
            if existe:
                print("la tabla existe dentro de la base de datos " + db)
                #columna
                # Se abre el documento schema.json para su lectura
                schemaFile = open(direccion + "/schema.json", "r")
                # Se lee el documento
                schemaText = schemaFile.read()
                schemaJSON = ast.literal_eval(schemaText)
                schemaFile.close()
                schemaData = schemaJSON['data']

                # Se guardan en variables el tamano de las filas y las columnas del schema.json
                num_rows = int(schemaJSON['registros'])
                num_columns = len(schemaData)

                schemaColumnas = []
                #sacar los nombres en un array
                for column in schemaData:
                    schemaColumnas.append(column['nombre'])
                #verificadar que las columnas esten dentro
                isIn = 0
                indices = {}
                todo = False
                # Si lo que el usuario desea es hacer un SELECT de todo (*)
                if len(columnas) == 1 and columnas[0] == "*":
                    todo = True
                    for data in schemaColumnas:
                        indices[data] = schemaColumnas.index(data)
                # Si el select contiene el nombre de las columnas que desea seleccionar
                else:
                    for datas in columnas:
                        if datas in schemaColumnas:
                            isIn = isIn + 1
                            indices[datas] = schemaColumnas.index(datas)
                #print(indices)
                if isIn == len(columnas) or todo:
                    print("estan dentro de la tabla ")
                    #sacar info
                    dataFile = open(direccion + "/data.txt", "r")
                    datatext = str(dataFile.read())
                    dataFile.close()

                    #Separar los datos de la base de datos por enter (cada objeto ingresado a la DB)
                    dataarray = datatext.split("\n")
                    #Hacer un array bidimensional para cada atributo de la DB
                    estructura = [[0 for x in range(int(num_columns))] for y in range(int(num_rows))]
                    #Llenar el array bidimensional con los datos de la DB
                    for y in range(0,len(dataarray)):
                        columns = dataarray[y].split('|')
                        for x in range(0,len(columns)-1):
                            estructura[y][x]= str(columns[x])

                    #resultado de select
                    resultado = ""
                    #print(indices)
                    for j in range(0, len(dataarray)):
                        cols = dataarray[j].split('|')
                        for i in range(0, len(cols) - 1):
                            for key, value in indices.items():
                                if value == i:
                                    resultado = resultado + " | " + estructura[j][i] + " | "
                        resultado = resultado + "\n"
                    #print(estructura)
                    respuesta.append(resultado);
                    #print(resultado)
                    #print (respuesta)
                else:
                    # Si lo que el usuario busca no esta dentro de la tabla
                    print("no estan dentro de la tabla ")

            # En caso que la tabla seleccionada no exista dentro de la base de datos
            else:
                print("la tabla " + tabla + " no existe dentro de la base de datos " + db)
        for i in respuesta:
            print (i);


    def exitDelete_stmt(self, ctx:sqlParser.Delete_stmtContext):
        global db
        if db != "":
            # Se crea una variable que almacene el nombre de la tabla proporcionada por el usuario
            tableName = ctx.table_name().getText()
            # Se establece la direccion donde esta guardada la tabla
            direccion = userpath + "/" + db +"/"+ tableName
            # Se crea una variable para almacenar los valores de la tabla
            tableValue = ctx.expr().getText()
            print(tableValue)

            #print("Table_name: " + tableName)
            #print("Table_Value: " + tableValue)

            #parsear los valores para obtener el nombre de columna, el condicional y el valor
            token = 0
            status = ""

            # Si el token que se encuentra es =
            token = tableValue.find("=")
            if(token != -1):
                valores_ingresados = tableValue.split("=")
                status = "="
            # Si el token que se encuentra es >
            token = tableValue.find(">")
            if(token != -1):
                valores_ingresados = tableValue.split(">")
                status = ">"
            # Si el token que se encuentra es <
            token = tableValue.find("<")
            if(token != -1):
                valores_ingresados = tableValue.split("<")
                status = "<"

            #print(valores_ingresados[0])
            #print(valores_ingresados[1])
            # Si el status es =, entonces se reemplazara por ==
            if status == "=":
                status = "=="
            #print(status)

            # Se abre el archivo schema.json para su lectura
            schema = open(direccion + "/schema.json", "r")
            # Se lee el archivo schema.json
            text = schema.read()
            json = ast.literal_eval(text)
            # Se establecen el numero de columnas y filas del archivo y se guardan en distintas variables
            num_columns = len(json['data'])
            num_rows = json['registros']
            schema.close()
            jsonColumn = json['data']
            jsonRegistros = int(json['registros'])
            numerocolumna = 0

            schemaColumnas = []
            #sacar los nombres en un array
            for column in jsonColumn:
                schemaColumnas.append(column['nombre'])

            # Se agregan valores a indices
            indices = {}
            if valores_ingresados[0] in schemaColumnas:
                indices[valores_ingresados[0]] = schemaColumnas.index(valores_ingresados[0])

            # Se abre el archivo data.txt para su lectura
            dataFile = open(direccion + "/data.txt", "r")
            # Se lee el archivo
            datatext = str(dataFile.read())
            dataFile.close()

            #Separar los datos de la base de datos por enter (cada objeto ingresado a la DB)
            dataarray = datatext.split("\n")
            #Hacer un array bidimensional para cada atributo de la DB
            estructura = [[0 for x in range(int(num_columns))] for y in range(int(num_rows))]
            #Llenar el array bidimensional con los datos de la DB
            for y in range(0,len(dataarray)):
                columns = dataarray[y].split('|')
                for x in range(0,len(columns)-1):
                    estructura[y][x]= str(columns[x])

            #resultado de select
            resultadoIndices = []
            # Se imprime la estructura del archivo data.txt
            print(estructura)

            # Se localizan las tuplas que se desean borrar
            for j in range(0, len(dataarray)):
                columns = dataarray[j].split('|')
                for i in range(0,len(columns)-1):
                    for key, value in indices.items():
                        if value == i:
                            if extra.where(estructura[j][i], status, valores_ingresados[1]):
                                #print(eval(str(estructura[j][i]) + status + valores_ingresados[1])==extra.where(estructura[j][i], status, valores_ingresados[1]))
                                resultadoIndices.append(int(j))
            resultado = ""
            borrar = ""
            respuesta = dataarray[:]
            # Se eliminan las tuplas desedas
            for j in range(len(dataarray) -1, -1, -1):
                for i in resultadoIndices:
                    if i == j:
                        borrar = borrar + dataarray[j] + "\n"
                        json['registros'] = ''+str(int(json['registros']) - 1)+''
                        print("pop: " + str(respuesta.pop(j)))
                    else:
                        resultado = resultado + dataarray[j] + "\n"

            #resultado = resultado[:-1]
            respuesta.pop(len(respuesta) - 1)
            print("respuesta " + str(respuesta) )

            #droppear la tabla
            newFile = open(direccion + "/data.txt", "w")
            #escribir en el archivo el nuevo string
            for item in respuesta:
                newFile.write(item + "\n")
            newFile.close()

            #Modificar el Schema para eliminar la columna tambien.
            newSchema = open(direccion + "/schema.json", "w")
            newSchema.write(str(json));
        # En caso de no tener una base de datos seleccionada
        else:
            print("No hay base de datos seleccionada")


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
