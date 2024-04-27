from Utils.variables import *
from Utils.functions import *
import time

os.system('cls')
print(os.getcwd())

noduplicated_product=pd.read_csv(r'.\src\Data\productos_scrape_240427.csv')
noduplicated_comments=pd.read_csv(r'.\src\Data\comentarios_scrape_240427.csv')
# noduplicated_product, noduplicated_comments = spider_amantis(url_secundaria,product_ingest,comment_ingest)
print("Vamos a proceder a la creación de los diferentes dataframes necesarios.")
df_product = product_engineer_function(noduplicated_product,product_engineer)
df_tag=tag_engineer_function(df_product,nombre_listas,tag_engineer)
df_prices=price_engineer_function (noduplicated_product,price_engineer)
df_comments,df_users=comments_engineer_function (noduplicated_comments,dm_mapping,comment_engineer,user_engineer)
print("Ya hemos creado los dataframes necesarios para su carga en la Base de Datos.")

time.sleep(5)
os.system('cls')
print("Gestionando su ingesta en la Base de Datos.\n Primero vamos a proceder a reindexar los dataframes.\nGenerando el mapeo necesario.")
map_product, map_product_out=mapeo_productos(BBDD, df_product)
map_user, map_user_out=mapeo_usuarios(BBDD, df_users)
df_prices_new=reengineer_price(BBDD, df_prices)

print("Mapeo realizado.\nReduciendo los registros a guardar.")
df_product_new=reindex(BBDD,"PRODUCT",df_product,"URL","LISTA_URL","right","ID_")
df_tag_new=reindex(BBDD,"PRODUCT",df_tag,"URL","LISTA_URL","right","ID_")
df_tag_new=df_tag_new[campos_tags]
df_users_new=reindex(BBDD,"USERS",df_users,"USERS","USERS","right","ID")
# print(df_comments.info())
df_comment_new=reengineer_comment(BBDD,df_comments)

print("Realizando la indexacion.")
df_product_new=df_product_new.replace({'ID': map_product})
df_product_new=df_product_new.iloc[:,:6]
df_tag_new=df_tag_new.replace({'ID': map_product})
df_prices_new=df_prices.replace({'ID_PRODUCT': map_product})
df_users_new=df_users_new.replace({'ID_USERS': map_user})
df_users_new=df_users_new.iloc[:,[1,0]]
df_comment_new=df_comment_new.replace({'ID': map_product})
df_comment_new=df_comment_new.replace({'ID_USERS': map_user})

print("vamos a guardar los ficheros antes de ingresarlos en la BBDD.")
df_product_new.to_csv(r'.\src\Data\new_products.csv',header=True,index=False)
df_tag_new.to_csv(r'.\src\Data\new_tags.csv',header=True,index=False)
df_prices_new.to_csv(r'.\src\Data\new_prices.csv',header=True,index=False)
df_users_new.to_csv(r'.\src\Data\new_users.csv',header=True,index=False)
df_comment_new.to_csv(r'.\src\Data\new_comments.csv',header=True,index=False)

print ("Guardando los datos en la BBDD.")
ingesta_datos (df_product_new,BBDD,"PRODUCT")
ingesta_datos (df_tag_new,BBDD,"TAGS")
ingesta_datos (df_prices_new,BBDD,"PRICES")         
ingesta_datos (df_users_new,BBDD,"USERS")
ingesta_datos (df_comment_new,BBDD,"COMMENT") 

print ("Se ha terminado el proceso de ingreso de datos.\nMuchas gracias")

