from datetime import datetime
import os

'''Variables URL'''

url_principal="https://www.amantis.net/"                        
url_secundaria=url_principal+'productos-amantis/'


date=str(datetime.today().strftime('%y%m%d'))
folder_ingest=r'.\src\Data'

'''Variables FICHEROS'''
ext=r'.csv'
scrape='_scrape'
file_product=r'\productos'
file_user=r'\usuarios'
file_comment=r'\comentarios'
file_price=r'\precios'
file_tag=r'\tags'
file_ingest_product=file_product+scrape
file_ingest_comment=file_comment+scrape
path=r'\Javier\Repositorios\Proyecto_tienda_online'
path_dashboard=r'src\Data\Dashboard'
new_path=os.chdir(os.getcwd()+path)


product_ingest=folder_ingest+file_ingest_product+'_'+date+ext
comment_ingest=folder_ingest+file_ingest_comment+'_'+date+ext
product_engineer=folder_ingest+file_product+'_'+date+ext
tag_engineer=folder_ingest+file_tag+'_'+date+ext
price_engineer=folder_ingest+file_price+'_'+date+ext
comment_engineer=folder_ingest+file_comment+'_'+date+ext
user_engineer=folder_ingest+file_user+'_'+date+ext

'''Variables BBDD'''
BBDD=r".\src\Resources\online_shop.db"
nombre_listas=['amenities','anal','BDSM','femenino','masculino','juguetes','lenceria','muebles']
campos_tags=nombre_listas.copy()
campos_tags.insert(0,'ID')

'''Mapeo de meses'''
dm_mapping={
    'enero':1, 
    'febrero':2, 
    'marzo':3, 
    'abril':4, 
    'mayo':5,
    'junio':6, 
    'julio':7,
    'agosto':8, 
    'septiembre':9, 
    'octubre':10, 
    'noviembre':11, 
    'diciembre':12,
} 

