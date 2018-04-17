import sys
from antlr4 import *
from HelloLexer import HelloLexer
from HelloParser import HelloParser
from HelloListener import HelloListener
from antlr4.error.ErrorListener import ErrorListener

class SayHelloListener(HelloListener):
    def exitR(self, ctx):
        print("Hello world. At the input has been already validated")

class ParserException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParserExceptionErrorListener(ErrorListener):
    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise ParserException("line " + str(line) + ":" + str(column) + " " + msg)

def parse(text):
    lexer = HelloLexer(InputStream(text))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParserExceptionErrorListener())

    stream = CommonTokenStream(lexer)

    parser = HelloParser(stream)
    parser.removeErrorListeners()
    parser.addErrorListener(ParserExceptionErrorListener())

    tree = parser.r()

    # Luego de procesar, visitar el arbol construido con el custom listener
    # definido arriba
    sayHelloListener = SayHelloListener()
    walker = ParseTreeWalker()
    walker.walk(sayHelloListener, tree)

'''
Uso: python cli.py

De acuerdo con la gramatica definida en Hello.g4, las unicas consturcciones validas
son aquellas que inician con la palabra 'hello' y son seguidas por identificadores
en minusculas o espacios en blanco. Cualquier otra construccion debera levantar una
ParserException.
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
