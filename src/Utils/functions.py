'''Importamos los modulos necesarios para que las funciones funcionen'''
import pandas as pd
import numpy as np
import pickle
import re
from bs4 import BeautifulSoup as bs
import requests
import spacy
from nltk.stem.snowball import SnowballStemmer
import datetime
import os
import sqlite3
from datetime import datetime,date


nlp = spacy.load('es_core_news_lg')

'''Creamos las funciones necesarias para la realización de los procesos'''

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
    '''Generar fichero pickle a partir de una lista de tareas. 
    
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


def cargar_listas_desde_pickles(nombres_archivos, carpeta):
    carpeta_absoluta=os.path.abspath(carpeta)
    listas_pickle = {}
    lista_pickle=None
    
    for nombre_archivo in nombres_archivos:
        try:
            ruta_archivo = os.path.join(carpeta_absoluta, nombre_archivo + ".pickle")
        
            with open(ruta_archivo, "rb") as archivo_pickle:
                lista_pickle = pickle.load(archivo_pickle)
        except  FileNotFoundError:
            print(f"Error: archivo no encontrado: {ruta_archivo}")
        except Exception as e:
            print(f"Error al cargar el archivo{ruta_archivo}: {e}")
            continue

        # Almacena la lista en el diccionario
        listas_pickle[nombre_archivo] = lista_pickle
        
    return listas_pickle

def aplicar_funcion_a_columna(df, listas, nombre_listas, columna="DESCRIPTION"):
    '''
    Función que aplica la función extraer lemas a la columna DESCRIPTION
    La función tiene como parametros de entrada:
    Dataframe
    Diccionario con las listas a tratar
    Columna sobre la que realizar la separación.
    '''

    # Carga la lista desde el diccionario
    lista = listas[nombre_listas]

    # Aplica la función a la columna del DataFrame
    df[nombre_listas] = df[columna].apply(lambda x: any(lematizado in lista for lematizado in extraer_lemas(x)))

    return df


'''Creamos las funciones de los scripts'''

def spider_amantis(url,product_ingest,comment_ingest):
    """
    Obtiene las URLs de los productos para luego extraer la información de cada producto.

    Input:
    - url (str): URL base para la extracción de productos.
    - product_ingest (str): Nombre de fichero de productos.
    - comment_ingest (str): Nombre de fichero de comentarios.


    Return:
    - noduplicated_product (pd.DataFrame): DataFrame de productos sin duplicados.
    - noduplicated_comments (pd.DataFrame): DataFrame de comentarios sin duplicados.
    """
    
    pages= np.arange(1,25)  
    '''Listas a generar con la información de los productos'''
    lista_URLs = []
    name=[]
    regular_prices=[]
    new_price=[]
    info=[]
    id=[]
    comentarios=[]
    '''Generamos 2 diccionarios con los datos importantes para ingresar en una BBDD'''

    diccionario_datos_productos={"ID":id,"NAME":name,"INFO":info,"LISTA_URL":lista_URLs,"REGULAR_PRICE":regular_prices,"DISCOUNT_PRICE":new_price}

    diccionario_comentarios_productos={"ID":id,"COMENTARIOS":comentarios}

    '''Listas para generar la información de los comentarios'''
    id_comment=[]
    comments=[]
    date=[]
    ratio=[]
    users=[]
    comment=[]

    print("Empezando a recoger datos de las paginas")
    for page in pages:
        
        if page == 1:
            URL = url
            response = requests.get(url)
            soup = bs(response.text, 'lxml')
            productos = soup.find_all(class_='caption')
            for producto in productos[9:]:
                URL_producto = producto.find('a')['href']
                lista_URLs.append(URL_producto)
            
        else:
            URL = url+'page' + str(page)+'/'
            response = requests.get(URL)
            soup = bs(response.text, 'lxml')
            productos = soup.find_all(class_='caption')
            for producto in productos[9:]:
                URL_producto = producto.find('a')['href']
                lista_URLs.append(URL_producto)
    print("Terminando de recoger los datos de los links de las paginas\nEmpezando a generar las listas de los productos")

    for i in range(len(lista_URLs)):
        id.append(i)

        
    '''Extraemos la información de cada producto existente'''

    for URL in lista_URLs:
        url_product=URL
        response_product = requests.get(url_product)
        soup_product = bs(response_product.text, 'lxml')
        user_comments_product=[]
        date_comments_product=[]
        comments_product=[]
        rating=[]

        titulos=soup_product.find_all("h1",class_="h3")
        for titulo in titulos:
            nombre=titulo.get_text(strip=True)
            name.append(nombre)

        all_price = soup_product.find_all("div", class_="productoPrecio pull-right tdd_precio")                        
        for price_container in all_price:                                                                    
            try:
                special_price = price_container.find("span", class_="productSpecialPrice")
                if special_price:
                    item_price = float(special_price.get_text(strip=True).replace(",", ".").split('€')[0])
                    new_price.append(item_price)
                    regular_price = price_container.find("del").get_text(strip=True)
                    item_regular_price = float(regular_price.replace(",", ".").split('€')[0])
                    regular_prices.append(item_regular_price)
                else:
                    regular_price = price_container.find("span").get_text(strip=True)
                    item_regular_price = float(regular_price.replace(",", ".").split('€')[0])
                    new_price.append(item_regular_price)
                    regular_prices.append(None)
            except:
                new_price.append(None)
                regular_prices.append(None)

        description=soup_product.find("div", class_="description") 
        information=description.get_text().split('\n')[1:]
        documentation = ''.join(information)
        info.append(documentation)


        '''Vamos a obtener los datos de los comentarios de los usuarios'''

        all_user_comments = soup_product.find_all("span", class_="name-user") 
        for user_comment in all_user_comments:
            user_comments_product.append(user_comment.get_text(strip=True))

        
        all_dates = soup_product.find_all("span", class_="date")  
        for dates in all_dates:
            dates_text=dates.get_text(strip=True)
            date_comments_product.append(dates_text)

        all_comments = soup_product.find_all("p")
        for formats in all_comments[-len(date_comments_product):]:
            comments_product.append(formats.get_text(strip=True))

        hearts = soup_product.find_all('div', class_= 'box-description')
        for heart in hearts:
            heart_rating = heart.find_all('span', class_= 'fas fa-heart')
            num_hearts = len(heart_rating)
            rating.append(num_hearts)

        datos = list(zip(date_comments_product,rating, user_comments_product,comments_product ))
        comentarios.append(datos)

    for i, regular_price in enumerate(regular_prices):
        if regular_price is None:
            regular_prices[i] = new_price[i]

    print("Realizando la ingenieria de los datos\nEliminando duplicados de productos")
    productos=pd.DataFrame(diccionario_datos_productos)
    noduplicated_product = productos.drop_duplicates(subset='NAME', keep='first')
    removed_id = productos[productos.duplicated(subset='NAME', keep='first')]['ID']

    print("Tratando los comentarios")

    comentarios_productos=pd.DataFrame(diccionario_comentarios_productos)
    comentarios=pd.DataFrame()
    diccionario={"id":id_comment,"comments":comments}

    for id_product,n_comments in enumerate (comentarios_productos['COMENTARIOS']):
        for i in n_comments:
            id_comment.append(id_product)
            comments.append(i)


    for j in range(len(diccionario['comments'])):
        date.append(diccionario['comments'][j][0])
        ratio.append(diccionario['comments'][j][1])
        users.append(diccionario['comments'][j][2])
        comment.append(diccionario['comments'][j][3])


    comentarios['ID']=pd.Series(id_comment)
    comentarios['DATE']=pd.Series(date)
    comentarios['RATIO']=pd.Series(ratio)
    comentarios['USERS']=pd.Series(users)
    comentarios['COMMENT']=pd.Series(comment)
    
    noduplicated_comments = comentarios[~comentarios['ID'].isin(productos[productos['ID'].isin(removed_id)]['ID'])]
    
    h=input("Quieres salvar los datos de la descarga?").upper()
    if h=="SI":
        noduplicated_product.to_csv(product_ingest,header=True,index=False)           # Tengo que generar el path correcto
        noduplicated_comments.to_csv(comment_ingest,header=True,index=True)           # Tengo que generar el path correcto
    
    return noduplicated_product,noduplicated_comments

def product_engineer_function (df_product,product_engineer):
    """
    Realiza ingeniería de datos para la creación de la tabla de productos.

    Input:
    - df_product (pd.DataFrame): DataFrame de productos.
    - product_engineer (str): Nombre de fichero generado.

    Return:
    - df_product (pd.DataFrame): DataFrame de productos con ingeniería aplicada.
    """
    
    print("Generando el fichero de productos")
   
    df_product['NAME'] = df_product['NAME'].str.replace(r'-(?=\w)', '_')
    df_product[['PRODUCT', 'SLOGAN']] = df_product['NAME'].str.split('[,-.]', 1, expand=True)
    df_product['PRODUCT'] = df_product['PRODUCT'].str.strip()
    df_product['SLOGAN'] = df_product['SLOGAN'].str.strip()
    df_product['CHARACTERISTICS'] = df_product['INFO'].str.split('Ver características y medidas|Características', 1).str[1]
    df_product['DESCRIPTION'] = df_product['INFO'].str.split('Ver características y medidas|Características', 1).str[0].str.strip()
    df_product['CHARACTERISTICS'] = df_product['CHARACTERISTICS'].str.replace('\r', ' ')
    df_product['DESCRIPTION'] = df_product['DESCRIPTION'].str.replace('\r', ' ')

    col_1 = df_product.pop('PRODUCT')
    col_2=df_product.pop('SLOGAN')
    col_3=df_product.pop('DESCRIPTION')
    col_4=df_product.pop('CHARACTERISTICS')

    df_product.drop(columns=['NAME'],inplace=True)
    df_product.drop(columns=['INFO'],inplace=True)

    df_product.insert(loc= 1 , column= 'PRODUCT', value= col_1)
    df_product.insert(loc= 2 , column= 'SLOGAN', value= col_2)
    df_product.insert(loc= 3 , column= 'DESCRIPTION', value= col_3)
    df_product.insert(loc= 4 , column= 'CHARACTERISTICS', value= col_4)
    df_product=df_product.iloc[:,:6]

    h=input("Quieres salvar los datos del dataframe de productos?").upper()
    if h=="SI":
        df_product.to_csv(product_engineer,header=True,index=False)          
        
    return df_product

def tag_engineer_function (df_tag,nombre_listas,tag_engineer):
    """
    Realiza ingeniería de datos para la creación de la tabla de tags.

    Input:
    - df_tag (pd.DataFrame): DataFrame de productos.
    - nombre_lista (Lista): Lista de nombres utilizada para cargar listas desde pickles.
    - tag_engineer (str): Nombre de fichero generado.


    Return:
    - df_tag (pd.DataFrame): DataFrame de tags de los diversos productos.
    """
    from Utils.functions import eliminacion_acentos,cargar_listas_desde_pickles,aplicar_funcion_a_columna
    print("Generando el fichero de tags")
    df_tag['SLOGAN'] = df_tag['SLOGAN'].str.lower()
    df_tag['DESCRIPTION'] = df_tag['DESCRIPTION'].str.lower()
    # df_tag['SLOGAN'] = df_tag['SLOGAN'].apply(eliminacion_acentos)              # Esto da error, debe de ser porque hay NaN en el campo
    df_tag['DESCRIPTION'] = df_tag['DESCRIPTION'].apply(eliminacion_acentos)

    listas = cargar_listas_desde_pickles(nombre_listas,"src\\Utils")
    for nombre_lista in listas:
        df_tag = aplicar_funcion_a_columna(df_tag, listas,nombre_lista)

    # Queda pendiente eliminar las columnas que no son necesarias
        
        
    h=input("Quieres salvar los datos del dataframe de tags?").upper()
    if h=="SI":
        df_tag.to_csv(tag_engineer,header=True,index=False)           # Tengo que generar el path correcto

    return df_tag

def price_engineer_function (dataframe,price_engineer):
    """
    Realiza ingeniería de datos para la creación de la tabla de precios.

    Input:
    - dataframe (pd.DataFrame): DataFrame de productos.
    - price_engineer (str): Nombre de fichero generado.

    Return:
    - df_prices (pd.DataFrame): DataFrame de precios de los productos.
    """

    print("Generando el fichero de precios")
    col_1 = dataframe.pop('REGULAR_PRICE')
    col_2=dataframe.pop('DISCOUNT_PRICE')
    col_3=dataframe['ID']
    dataframe.insert(loc= 1 , column= 'ID_PRODUCT', value= col_3)
    dataframe.insert(loc= 2 , column= 'REGULAR_PRICE', value= col_1)
    dataframe.insert(loc= 3 , column= 'DISCOUNT_PRICE', value= col_2)
    date_price=datetime.today().strftime('%y/%m/%d')
    df_prices=dataframe.iloc[:,:4]
    df_prices['FECHA']=date_price

    h=input("Quieres salvar los datos del dataframe de precios?").upper()
    if h=="SI":
        df_prices.to_csv(price_engineer,header=True,index=False)           # Tengo que generar el path correcto
    return df_prices

def comments_engineer_function (dataframe,dm_mapping,comment_engineer,user_engineer):
    """
    Realiza ingeniería de datos para la creación de la tabla de comentarios.

    Input:
    - dataframe (pd.DataFrame): DataFrame de comentarios.
    - dm_mapping (dict): Mapeo de meses para el tratamiento de fechas.

    Return:
    - df_comments (pd.DataFrame): DataFrame de comentarios con ingeniería aplicada.
    - df_users (pd.DataFrame): DataFrame de usuarios que han realizado comentarios.
    """

    '''Tratando a los Usuarios'''
    print("Generando el fichero de usuarios")

    # nombre_count = {}
    count = {}

    for i, row in dataframe.iterrows():
        id = row['ID']
        nombre = row['USERS']
        
        if id not in count:
            count[id] = {}
            
        if nombre in count[id]:
            count[id][nombre] += 1
            nueva_nombre = f"{nombre}_{count[id][nombre]}"
            dataframe.loc[i, 'USERS'] = nueva_nombre
        else:
            count[id][nombre] = 1
    lista_users=dataframe['USERS'].unique()
    mivalor = [ x for x in range(len(lista_users))]             
    lista_users=list(lista_users)                                
    lista_users_code = {k: v for k, v in zip(lista_users, mivalor)}   
    dataframe['ID_USERS']= dataframe['USERS'].map(lista_users_code)
    df_users=pd.DataFrame()
    df_users['ID_USERS']=dataframe['ID_USERS']
    df_users['USERS']=dataframe['USERS']
    df_users.drop_duplicates(subset='ID_USERS', keep='first',inplace=True)

    print("Generando el fichero de comentarios")
    '''Tratando a las Fechas'''

    dataframe['DAY']=dataframe['DATE'].str.split(' ').str.get(1).astype('Int64')
    dataframe['MONTH']=dataframe['DATE'].str.split(' ').str.get(2).str.split(',').str.get(0)
    dataframe['YEAR']=dataframe['DATE'].str.split(' ').str.get(-1).astype('Int64')
    dataframe['MONTH']=dataframe['MONTH'].map(dm_mapping)
    dataframe['DATE'] = pd.to_datetime(dataframe.iloc[:,-3:])
    df_comments=dataframe.iloc[:,:-3]

    col = df_comments.pop('ID_USERS')
    df_comments.drop(columns=['USERS'],inplace=True)
    df_comments.insert(loc= 4 , column= 'ID_USERS', value= col)

    h=input("Quieres salvar los datos de usuarios y comentarios?").upper()
    if h=="SI":
        df_users.to_csv(user_engineer,header=True,index=False)           # Tengo que generar el path correcto
        df_comments.to_csv(comment_engineer,header=True,index=False)           # Tengo que generar el path correcto

    return df_comments,df_users


# BBDD="src/Resources/online_shop.db"

def reengineer_comment(BBDD, df_comment):
    """Reducción del número de registros del dataframe commentarios para su ingesta en la BBDD
    Input:
    - BBDD (str): Base de datos utilizada.
    - df_comment (Dataframe): Dataframe con los comentarios originales, sin filtrar

    Return:
    - df_comment_filtered (Dataframe): Dataframe con los comentarios filtrados y las fechas convertidas en Datetime.
    """

    print("Iniciando la conversión de fecha y la reducción de datos de comentarios")

    ''' Conectamos con la base de datos, extraemos la fecha más reciente y la cerramos'''
    from Utils.functions import sql_query
 
    
    conn = sqlite3.connect(BBDD)
    cursor = conn.cursor()
    query='''SELECT MAX(DATE) FROM COMMENT'''
    MAX_date=sql_query(query,cursor)
    conn.commit()
    cursor.close()
    conn.close()
    fecha_maxima = MAX_date.iloc[0, 0]
    fecha_maxima = date.fromisoformat(fecha_maxima)
    max_date = pd.to_datetime(fecha_maxima, unit='ns')

    '''Cargamos el dataframe y lo preparamos para su filtro'''
    df_comment['DATE']=pd.to_datetime(df_comment['DATE'])
    df_comment_filtered=df_comment[df_comment['DATE']> max_date]
    print("El número de registros a ingresar es:",len(df_comment_filtered))
    return df_comment_filtered

def mapeo_productos(BBDD, df_product):
    """Función para la conversión de los ID de productos

   Input:
      - BBDD (str): nombre de la BBDD.

      - df_product (dataframe): Dataframe donde se encuentran los productos

   Output:
      - map_product (Dict): Diccionario con los ID mapeados coincidentes.
       
      - map_product_out (Dict): Diccionario con los ID mapeados no coincidentes.
    """
    from Utils.functions import sql_query

    print("Vamos a reindexar los productos")


    '''Llamando a la base de datos para extraer información'''
    conn = sqlite3.connect(BBDD)
    cursor = conn.cursor()
    query='''SELECT ID,URL FROM PRODUCT'''
    df_bbdd=sql_query(query,cursor)
    conn.commit()
    cursor.close()
    conn.close()

    '''Obteniendo los ID coincidentes'''

    df_products_in = pd.merge(df_bbdd, df_product, left_on="URL", right_on="LISTA_URL")
    id_product_BBDD = df_products_in['ID_x'].tolist()
    id_product_new = df_products_in['ID_y'].tolist()
    map_product = {k: v for k, v in zip(id_product_BBDD, id_product_new)}

    '''Obteniendo los ID NO coincidentes'''
    max_id_product=df_bbdd['ID'].max()
    df_product_out = pd.merge(df_bbdd, df_product, left_on="URL", right_on="LISTA_URL",how='right',suffixes=('_',''))
    df_product_out = df_product_out[df_product_out['URL'].isnull()]
    id_product_out = df_product_out['ID'].tolist()
    df_product_out = df_product_out[df_product.columns]
    df_product_out['ID'] = max_id_product + 1 + df_product_out.index
    id_new_product = df_product_out['ID'].tolist()
    map_product_out = {k: v for k, v in zip(id_new_product,id_product_out )}
    map_product.update(map_product_out)

    return map_product, map_product_out

def mapeo_usuarios(BBDD, df_user):
   '''Función para la conversión de los ID de productos

   Input:
      - BBDD (str): nombre de la BBDD.

      - df_user (dataframe): Dataframe donde se encuentran los productos

   Output:
      - map_user (Dict): Diccionario con los ID mapeados coincidentes.
       
      - map_user_out (Dict): Diccionario con los ID mapeados no coincidentes.
   '''

   print("Vamos a reindexar los usuarios")

   from Utils.functions import sql_query

   '''Llamando a la base de datos para extraer información'''
   conn = sqlite3.connect(BBDD)
   cursor = conn.cursor()
   query='''SELECT ID,USERS FROM USERS'''
   df_bbdd=sql_query(query,cursor)
   conn.commit()
   cursor.close()
   conn.close()

   '''Obteniendo los ID coincidentes'''

   df_users_in = pd.merge(df_bbdd, df_user, left_on="USERS", right_on="USERS")
   id_user_BBDD = df_users_in['ID'].tolist()
   id_user_new = df_users_in['ID_USERS'].tolist()
   map_user = {k: v for k, v in zip(id_user_BBDD, id_user_new)}

   '''Obteniendo los ID NO coincidentes'''
   max_id_user=df_bbdd['ID'].max()
   df_users_out = pd.merge(df_bbdd, df_user, left_on="USERS", right_on="USERS",how='right',suffixes=('_',''))
   df_users_out = df_users_out[df_users_out['ID'].isnull()]
   id_user_out = df_users_out['ID_USERS'].tolist()
   df_users_out = df_users_out[df_user.columns]
   df_users_out['ID'] = max_id_user + 1 + df_users_out.index
   id_new_user = df_users_out['ID'].tolist()
   map_user_out = {k: v for k, v in zip(id_new_user,id_user_out )}
   map_user.update(map_user_out)

   return map_user, map_user_out

def reindex (BBDD,FIELD,df, join_left,join_right,how,ID,map):              # Vamos a probar con outer
    '''
    Función para reducir el numero de registros del dataframe product o users.

    Input:
    - BBDD (str): Dirección de la base de datos.
    - FIELD (str): Nombre de la tabla de la base de datos.
    - df (pd.DataFrame): DataFrame con registros a reducir.
    - join_left (str): Nombre de la columna de dataframe_BBDD donde hacer join.
    - join_right (str): Nombre de la columna de df donde hacer join.
    - how (str): Tipo de join.
    - ID (str): indice a sustituir.
    - map (Dict): Diccionario donde están la relación de indices a sustituir 

    Return:
    - df_new (pd.DataFrame): DataFrame con registros reindexados y reducidos.
    '''

    from Utils.functions import sql_query

    '''Llamando a la base de datos para extraer información'''
    conn = sqlite3.connect(BBDD)
    cursor = conn.cursor()
    query=f"SELECT * FROM {FIELD}"
    df_bbdd=sql_query(query,cursor)
    conn.commit()
    cursor.close()
    conn.close()

    '''Realizando el JOIN de los dataframes'''
    df_new = pd.merge(df_bbdd, df,left_on=join_left, right_on=join_right,how=how,suffixes=('_',''))
    df_new=df_new[df_new[join_left].isnull()]
    df_new.replace({ID:map},inplace=True)
    return df_new


def reindex_2 (BBDD,FIELD,df, join_left,join_right,how,ID,map,list):
    '''
    Función para reducir el numero de registros del dataframe tags.

    Input:
    - BBDD (str): Dirección de la base de datos.
    - FIELD (str): Nombre de la tabla de la base de datos.
    - df (pd.DataFrame): DataFrame con registros a reducir.
    - join_left (str): Nombre de la columna de dataframe_BBDD donde hacer join.
    - join_right (str): Nombre de la columna de df donde hacer join.
    - how (str): Tipo de join.
    - ID (str): indice a sustituir.
    - map (Dict): Diccionario donde están la relación de indices a sustituir 
    - list (list): Lista con los nombres de las columnas a permanecer.

    Return:
    - df_new (pd.DataFrame): DataFrame con registros reindexados y reducidos.
    '''
    from Utils.functions import sql_query

    '''Llamando a la base de datos para extraer información'''
    conn = sqlite3.connect(BBDD)
    cursor = conn.cursor()
    query=f"SELECT * FROM {FIELD}"
    df_bbdd=sql_query(query,cursor)
    conn.commit()
    cursor.close()
    conn.close()

    '''Realizando el JOIN de los dataframes'''
    df_new = pd.merge(df_bbdd, df,left_on=join_left, right_on=join_right,how=how,suffixes=('_',''))
    df_new=df_new[df_new[join_left].isnull()]
    df_new.replace({ID:map},inplace=True)
    df_new=df_new[list]
    return df_new

def ingesta_datos (df,BBDD,TABLE):
    '''
    Función para la incorporación de los nuevos datos en una base de datos.
    Es una formula general que es independiente del numero de campos que tenga la BBDD.
    Input:
    - df (pd.DataFrame): DataFrame con registros a incluir.
    - BBDD (str): path y nombre completo de la Base de Datos.
    - TABLE (str): Nombre de la TABLA dentro de la Base de Datos.
    '''
    # from Utils.functions import sql_query

    conn = sqlite3.connect(BBDD)
    cursor = conn.cursor()

    num= len(df.columns)
    columnas=["?" for _ in range(num)]
    columnas_total=", ".join(columnas)

    lista_valores=df.values.tolist()
    cursor.executemany(f"INSERT INTO {TABLE} VALUES (columntas_total)",lista_valores)

    print (f"Se han ingresado los datos a la tabla {TABLE}")

    conn.commit()
    cursor.close()
    conn.close()