from Utils.variables import *
from Utils.functions import *

os.system('cls')

noduplicated_product, noduplicated_comments = spider_amantis(url_secundaria,product_ingest,comment_ingest)
print("Vamos a proceder a la creación de los diferentes dataframes necesarios.")
df_product = product_engineer_function(noduplicated_product,product_engineer)
df_tag=tag_engineer_function(df_product,nombre_listas,tag_engineer)
df_prices=price_engineer_function (noduplicated_product,price_engineer)
df_comments,df_users=comments_engineer_function (noduplicated_comments,dm_mapping,comment_engineer,user_engineer)
print("Ya hemos creado los dataframes necesarios para su carga en la Base de Datos.")

print("Gestionando su ingesta en la Base de Datos.\n Primero vamos a proceder a reindexar los dataframes")

map_product, map_product_out=mapeo_productos(BBDD, df_product)
map_user, map_user_out=mapeo_usuarios(BBDD, df_product)
df_comment_new=reengineer_comment(BBDD,df_comments)
df_product_new=reindex(BBDD,df_product,"URL","LISTA_URL","right","ID",map_product)
df_users_new=reindex(BBDD,df_users,"USERS","USERS","right","ID",map_user)

#Aqui falta la función para reindexar tags

df_prices_new=reindex_2 (df_prices,'ID',map_product)
df_comment_new=reindex_2 (df_comment_new,'ID',map_product)
df_comment_new=reindex_2 (df_comment_new,'ID',map_user)

ingesta_datos (df_product_new,BBDD,PRODUCT)
ingesta_datos (df_users_new,BBDD,USERS)
ingesta_datos (df_tags_new,BBDD,TAGS)
ingesta_datos (df_prices_new,BBDD,PRICES)
ingesta_datos (df_comment_new,BBDD,COMMENT)

print ("Se ha terminado el proceso de ingreso de datos.\Muchas gracias")

