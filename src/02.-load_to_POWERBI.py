import os
import sqlite3
import csv
from Utils.variables import *

os.system('cls')
path=os.path.join(os.getcwd(),path_dashboard)

'''Creamos el cursor'''
conn = sqlite3.connect(BBDD)
cursor = conn.cursor()

'''Iteramos para obtener el nombre de todas las TABLES existentes'''
lista_tables=[]
res = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
for name in res:
    lista_tables.append(name[0])

'''Iteramos para obtener los datos y los nombres de los campos de cada TABLE '''

for table in lista_tables:
    cursor.execute(f"SELECT * FROM {table}")                            # Seleccionamos la información de los registros
    name=table+ext
    file=os.path.join(path,name)
    data=cursor.fetchall()                                              # Obtenemos los datos seleccionados
    names = [description[0] for description in cursor.description]

    '''Creamos el fichero .csv correspondiente'''

    print (f'Cargando el fichero {file}')
    with open (file,'w',newline='',encoding='utf-8') as archivo_csv:
        escritor_csv=csv.writer(archivo_csv)
        escritor_csv.writerow(names)
        for fila in data:
            escritor_csv.writerow(fila)
    archivo_csv.close()

'''Cerramos la base de datos'''

conn.commit()
cursor.close()
conn.close()