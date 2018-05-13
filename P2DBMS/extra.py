import os
import re
import datetime

def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]


def any_to_int (s):
    s = re.sub('[^0-9]', '', s)
    return s


def type_anything(texto_raw):
    try:
        #parseo a entero
        a = int(texto_raw)
        return "INT"
    except:
        try:
            #parseo a FLOAT
            b = float(texto_raw)
            return "FLOAT"
        except:
            #regreso del mismo texto en caso de que no encuentre nada
            #return texto_raw
            try:
                c = re.sub('-', '', texto_raw)
                d = datetime.datetime.strptime(c, '%Y%m%d').date()
                return "DATE"
                #tipo de variable datetime.date
            except:
                return "CHAR"
    else:
        return None


def try_date():
    print(datetime.date(year=2000, month=2, day=30))
    return 0

def where(valor, operador, comparador):
    if operador == "=":
        operador = "=="
    return eval(str(valor) + operador + str(comparador))
