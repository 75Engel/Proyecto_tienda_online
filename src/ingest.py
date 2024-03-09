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
df_comment_new=reengineer_comment(BBDD,df_comment)

# df_product_new = pd.merge(df_bbdd_product, df_product,left_on="URL", right_on="LISTA_URL",how='right',suffixes=('_',''))
# df_product_new = df_product_new[df_product_new['URL'].isnull()]
# df_product_new.replace({'ID':map_product},inplace=True)

# df_users_new = pd.merge(df_bbdd_user, df_users, left_on="USERS", right_on="USERS",how='right',suffixes=('_',''))
# df_users_new = df_users_new[df_users_new['URL'].isnull()]
# df_users_new.replace({'ID':map_user},inplace=True)

# df_prices_new=df_prices.replace({'ID':map_product},inplace=True)

# df_comment_new=reengineer_comment("Resources/online_shop.db",df_comment)
# df_comment_new.replace({'ID_PRODUCT':map_product},inplace=True)
# df_comment_new.replace({'ID_USERS':map_user},inplace=True)

# # '''Reducimos los datos de tags y reindexamos'''
# # df_tags_new = pd.merge(df_bbdd_product, df_tags,left_on="URL", right_on="LISTA_URL",how='right',suffixes=('_',''))
# # df_tags_new = df_tags_new[df_tags_new['ID'].isnull()]
# # df_tags_new.replace({'ID':map_product},inplace=True)

# lista_valores_product = df_product_new.values.tolist()
# cursor.executemany("INSERT INTO PRODUCTS VALUES (?,?,?,?,?,?)", lista_valores_product)

# lista_valores_users = df_users_new.values.tolist()
# cursor.executemany("INSERT INTO USERS VALUES (?,?)", lista_valores_users)

# lista_valores_prices = df_prices_new.values.tolist()
# cursor.executemany("INSERT INTO PRICES VALUES (?,?,?,?,?)", lista_valores_prices)

# lista_valores_comments = df_comment_new.values.tolist()
# cursor.executemany("INSERT INTO COMMENTS VALUES (?,?,?,?,?,?)", lista_valores_comments)

# # lista_valores_tags = df_tags_new.values.tolist()
# # cursor.executemany("INSERT INTO TAGS VALUES (?,?,?,?,?,?,?,?,?)", lista_valores_tags)



# conn.commit()
# cursor.close()
# conn.close()


