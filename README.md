# Proyecto_tienda_online
Proyecto para el estudio de una tienda online, donde se realizará una obtención de datos (Webscrapping), análisis de la información existente y predicción de sugerencias

---
## Descripción del proyecto:

1.- Se realizará una toma de datos existentes en una página web de una tienda online, mediante Webscrapping, obteniendo la información relevante del producto, precio sin rebajar y precio rebajado, datos de los diversos usuarios que han comentado sobre las compras realizadas de cada producto y su valoración.

2.- Posteriormente se realizará un estudio, que queda por definir en una primera parte, sobre los productos existentes. 

Para este estudio generaremos nuevas *features* en función de la descripción del producto.

En este estudio vamos a ver:
   - tipología de los productos que tienen un descuento,
   - cuántos comentarios tienen cada producto, por año, por día de la semana, por meses,
   - valoración de los productos por parte de los usuarios,
   - tipología de productos con más comentarios,

3.- Finalmente, procederemos a realizar un modelado para predecir intereses de los usuarios en función de sus comentarios.

Indicar que dado que no se dispone de la información real sobre el número de usuarios que tiene la página web y las compras reales que se han realizado, se supondrá que sólo realizan compras los usuarios que han comentado y que además el 100% de las compras realizadas son las comentadas. 

Tendremos en cuenta que hay varios nombres de usuarios que se repiten, por lo que intentaremos discriminar de algún modo para poder asignar usuarios distintos en caso de que haya nombres repetidos dentro de un mismo producto, modificando el nombre del mismo.

Esto es importante de valorar, dado que no disponemos de la información interna de la tienda de la que vamos a obtener los datos.

Si se dispusiera de esta información, podríamos valorar el ratio de personas que emiten comentarios sobre los compradores reales para ver así cómo de activos son los usuarios de la página  web.


---
## Metodología:

En una primera parte se realizará un proceso de recopilación de la información de cada uno de los productos existentes.

En este punto utilizaremos *BeautifulSoup*, *Regex* y *NLP* para la obtención de los datos y generación de nuevas variables. 

En una segunda parte se creará una *BBDD* a través de la cual se realizarán las consultas necesarias para realizar el tratamiento de los datos y su visualización.

En este sentido, se utilizará un *Estudio Exploratorio de Datos* (EDA) usando las librerías de *numpy*, *pandas*, *matplolib* y *seaborn* de Python y se utilizará *PowerBI* para la creación de *Dashboards* interactivos de los KPIs más interesantes para tener una visión de la evolución de la página web.

En una tercera parte se utilizará la librería de *Sklearn* para realizar modelado de los datos. 
