from Utils.variables import *
from Utils.functions import *


noduplicated_product, noduplicated_comments = spider_amantis(url_secundaria,product_ingest,comment_ingest)
print("Vamos a proceder a la creación de los diferentes dataframes necesarios.")
df_product = product_engineer_function(noduplicated_product,product_engineer)
df_tag=tag_engineer_function(df_product,nombre_listas,tag_engineer)
df_prices=price_engineer_function (noduplicated_product,price_engineer)
df_comments,df_users=comments_engineer_function (noduplicated_comments,dm_mapping,comment_engineer,user_engineer)
print("Ya hemos creado los dataframes necesarios para su carga en la Base de Datos.\nGestionando su ingesta.")


