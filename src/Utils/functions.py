'''Importamos los modulos necesarios para que las funciones funcionene'''

import pandas as pd
import numpy as np
import pickle
import re
import spacy
from nltk.stem.snowball import SnowballStemmer
import datetime
nlp = spacy.load('es_core_news_lg')


def sql_query(query,cursor):
    ''''
    Generación de dataframe a partir de una base de datos
    '''

    # Ejecuta la query
    cursor.execute(query)

    # Almacena los datos de la query 
    ans = cursor.fetchall()

    # Obtenemos los nombres de las columnas de la tabla
    names = [description[0] for description in cursor.description]

    return pd.DataFrame(ans,columns=names)


def extraer_lemas(texto):
    '''
    Función que recibe un texto como entrada y devuelve una lista de los lemas de las palabras que se encuentran en el texto. 
    
    La lista de lemas resultante solo contiene las palabras que son alfabéticas, es decir, que no contienen caracteres numéricos o especiales.

    - Inputs:

        texto (str): Texto del cual se quieren extraer los lemas.

    - Outputs:

        lemas (list): Lista de los lemas de las palabras alfabéticas que se encuentran en el texto.
    '''
    doc = nlp(texto)
    lemas = [token.lemma_ for token in doc if token.is_alpha]
    return lemas

def eliminacion_acentos(text):
    '''
    Función que recibe un texto como entrada y devuelve el mismo texto pero sin los caracteres acentuados. 
    
    Para esto, se utiliza una expresión regular y un diccionario con los caracteres a reemplazar.

    - Inputs:

       text (str): Texto del cual se quieren eliminar los acentos.

    - Outputs:

        texto (str): Texto sin los caracteres acentuados.
         '''
    pattern = '[áéíóúÁÉÍÓÚ]'
    replace = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'Á': 'A', 'É': 'E', 'Í': 'I', 'Ó': 'O', 'Ú': 'U'}
    return re.sub(pattern, lambda match: replace[match.group()], text)

def spanish_stemmer(x):
    '''
    Función que recibe una cadena de texto como entrada y devuelve el texto procesado con un algoritmo de stemming en español. 
    
    El stemming es una técnica de NLP que busca reducir una palabra a su raíz. 
    
    En este caso, se utiliza el algoritmo SnowballStemmer de la librería NLTK.

    - Inputs:

        x (str): Texto que se quiere procesar con el algoritmo de stemming.

    - Outputs:

        str: Texto procesado con el algoritmo de stemming.
        '''
    stemmer = SnowballStemmer('spanish')
    return " ".join([stemmer.stem(word) for word in x.split()])

def pickled_list(lista,nombre_archivo, carpeta="Utils"):
    '''Generar fichero picke a partir de una lista de tareas. 
    
    Los ficheros se guardan en la carpeta Utils.
    
    - INPUT: 
        - lista (list):         Nombre de la lista a guardar.

        - nombre_archivo (str): Nombre del fichero a generar

        - carpeta: Carpeta donde se va a guardar el fichero .pickle

    - OUTPUT: 
        - lista (.pickle): fichero .pickle donde esta almacenada la información de la lista.

    '''
    ruta_archivo = os.path.join(carpeta, nombre_archivo + ".pickle")

    with open(ruta_archivo, "wb") as archivo_pickle:
            pickle.dump(lista, archivo_pickle)
    return lista


def unpickled_list(lista,nombre_archivo, carpeta="Utils"):
    '''Generar lista de tareas que existen en la carpeta Utils.
    
    Definir tareas nuevas a agregar dentro de la lista en el caso de haber una nueva tarea a añadir.
    
    - INPUT: 
        - lista (list):         Nombre de la lista a guardar.

        - nombre_archivo (str): Nombre del fichero a generar

        - carpeta:              Carpeta donde se va a guardar el fichero .pickle
    
    - OUTPUT: 
        - lista (list):         Lista de productos
    '''
    
    ruta_archivo = os.path.join(carpeta, nombre_archivo + ".pickle")
    with open(ruta_archivo, "rb") as archivo_pickle:
        lista = pickle.load(archivo_pickle)

    return lista

def cargar_listas_desde_pickles(nombres_archivos, carpeta="Utils"):
    listas = {}

    for nombre_archivo in nombres_archivos:
        ruta_archivo = os.path.join(carpeta, nombre_archivo + ".pickle")

        with open(ruta_archivo, "rb") as archivo_pickle:
            lista = pickle.load(archivo_pickle)

        # Cierra el archivo
        archivo_pickle.close()

        # Almacena la lista en el diccionario
        listas[nombre_archivo] = lista

    # Devuelve el diccionario de listas
    return listas

def aplicar_funcion_a_columna(df, nombre_lista, columna="DESCRIPTION"):
    '''
    Función que aplica la función extraer lemas a la columna DESCRIPTION
    La función tiene como parametros de entrada:
    Dataframe
    Diccionario con las listas a tratar
    Columna sobre la que realizar la separación.
    '''
    # Carga la lista desde el diccionario
    lista = listas[nombre_lista]

    # Aplica la función a la columna del DataFrame
    df[nombre_lista] = df[columna].apply(lambda x: any(lematizado in lista for lematizado in extraer_lemas(x)))

    return df