# -*- coding: utf-8 -*-
"""
Created on Wed Apr  7 18:20:40 2021

@author: Marcelo
"""

'''
0- load packages, define initial variables
'''
#import os
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import datetime
#import time
import math
from pathlib import Path
from myfunctions import API_usage
#from myfunctions import GCP_Functions
from myfunctions import Data_Process
from myfunctions.tools import Tools
import argparse
import xlrd
import openpyxl
import sqlalchemy as sa


ap = argparse.ArgumentParser()
ap.add_argument("-i", "--date_ini", type=str, default='no',
	help="define initial date yyyy-mm-dd")
ap.add_argument("-f", "--date_fin", type=str, default='no',
	help="define final date yyyy-mm-dd")
args = vars(ap.parse_args())

Date_Ini = (args["date_ini"]) 
Date_Fin = (args["date_fin"]) 
#Date_Ini= '20190101'
#Date_Fin= datetime.date.today().strftime("%Y%m%d")

lista_actividades = ['FUMIGADA','SIEMBRA', 'RENOVADA', 'DESBROSADA', 'ABONADA', 'ENMIENDA', 'ROTOBO']

'''
1- Function for login to API, store token and get Clientes, lotes
'''
token = API_usage.login_api()
clientes = API_usage.get_clientes(token)
lotes = API_usage.get_lotes(token)
hatos = API_usage.get_hatos(token)

#leer ultimas filas de ubicacion_vacas y actividades_lotes del cliente

'''
esto debe cambiar por traer de SQL, ubicaciones, traslado_hatos, traslado_vacas, produccion_leche, actividades_lote
verificar HATOS flatantes en SQL vs el excel
'''

'''
1-b loop throught clientes and read lotes
'''
clientes = clientes[0:1]
#define user
for client in clientes:
    print (client)
    cliente = 'ID_CLIENTE-'+str(client)
    analysis_area = '../db_'+cliente+'/'


'''
2- read and join data
'''
#for cloud files //  Storage para subir datos que se usen a futuro en modelos, o para Dash
bucket_files = 'satellite_storage'
folder ='Data/Database/'
pref = 'db_lotes_seguimiento' #change this
dest= '../Data/Database_ready/'
#listar de Cloud storage
#objetos = list(GCP_Functions.list_all_blobs(bucket_files,prefix=folder+cliente+'/'+pref,delimiter='/')) #list fro cloud storage
destination = dest + cliente 
Path(destination).mkdir(parents=True, exist_ok=True) #create local pc folder
    
    
'''
3- read ext files
'''

#read external lotes management files
'''
this part will eb directly from DB later
option for local o cloud
option to start from 0 or to read a existing DB, get last records of each lote and keep counting
'''
#ext_data_folder='../../20190601-20200630-simijaca/Database/'+'external_files'
#seguimiento2 = pd.read_csv(ext_data_folder+'/'+'db_lotes_seguimiento.csv',thousands=',', encoding='ANSI')

#to read local file
seguimiento = pd.read_excel('D:/M4A/DB_RECODO/PROGRAMA SEGUIMIENTO INVERSIONES CAMACHO 2021.xlsm', sheet_name='BASE DE DATOS')
seguimiento = seguimiento.iloc[:,0:23]
seguimiento = seguimiento[seguimiento['FECHA ACTIVIDAD']>='2010-01-01']

#in case of local data
full_seguimiento, pasto_actual, otras_acti = Data_Process.seguimiento_lotes_proc(seguimiento, lotes, destination)

todas_actividades = sorted(set(full_seguimiento['ACTIVIDAD'].dropna()))
todos_hatos = sorted(set(full_seguimiento['HATO'].dropna()))
##########################################################################################################

'''
3a- running sums, counts, resets of activities by area
'''
full_seguimiento['lote_change'] = full_seguimiento['lote_id'] != full_seguimiento['lote_id'].shift(1)
full_seguimiento['actividad_change'] = (full_seguimiento['ACTIVIDAD'] == full_seguimiento['ACTIVIDAD'].shift(1)) #full_seguimiento['lote_change']==False and

#funciones de pastorio    
full_seguimiento['ultimo_pastoreo'], full_seguimiento['dias_sin_pastoreo'], full_seguimiento['dias_de_pastoreo'],\
full_seguimiento['ultimo_animales']=  full_seguimiento.apply(lambda row: Tools.actividades(row['lote_change'],
                                                                                           row['ACTIVIDAD'],
                                                                                           row['NUMERO ANIMALES'],
                                                                                           row['date'],
                                                                                           row['actividad_change']),
                                                             axis=1, result_type='expand').T.values

# using dictionary to convert specific columns
convert_dict = {
                'dias_sin_pastoreo': int,
                'dias_de_pastoreo': int, 
                'ultimo_animales': int 
               }

full_seguimiento = full_seguimiento.astype(convert_dict) 


#running sum - group by name, actividad == pastoreo (dias de pastoreo)
sin_pasto = Tools.run_sum_reset(full_seguimiento['dias_sin_pastoreo'])
con_pasto = Tools.run_sum_reset(full_seguimiento['dias_de_pastoreo'])

full_seguimiento.drop(columns=['dias_de_pastoreo', 'dias_sin_pastoreo'], inplace=True)
full_seguimiento = pd.concat([full_seguimiento,con_pasto, sin_pasto], axis=1)
full_seguimiento.loc[full_seguimiento['ultimo_pastoreo'] == 0, 'ultimo_pastoreo'] = np.nan
full_seguimiento.loc[full_seguimiento['ultimo_animales'] == 0, 'ultimo_animales'] = np.nan
#running avg - group by namee, actividad == pastoreo (unmero animales 
#fill na (pad) - group by (name) -> ultimo pastoreo, dias de pastoreo, numero animales , forraje
full_seguimiento.loc[:,['name_c','TIPO DE FORRAJE','ultimo_pastoreo',
                        'ultimo_animales','dias_de_pastoreo','dias_sin_pastoreo']] = \
    full_seguimiento.loc[:,['name_c','TIPO DE FORRAJE','ultimo_pastoreo',
                            'ultimo_animales','dias_de_pastoreo','dias_sin_pastoreo']].\
        groupby('name_c').apply(lambda group: group.fillna(method='pad'))

del(con_pasto, sin_pasto)
####################################################################################################

'''
3b - run sums de varias actividades
podria ser mas rapida y flexible la funcion

'''
full_seguimiento['dias_sin_fumigada'], full_seguimiento['dias_sin_siembra'],full_seguimiento['dias_sin_renovada'],\
full_seguimiento['dias_sin_desbrosada'],full_seguimiento['dias_sin_abonada'], full_seguimiento['dias_sin_enmienda'], \
full_seguimiento['dias_sin_rotobo'] = full_seguimiento.\
    apply(lambda row: Tools.otras_actividades(row['lote_change'],
                                              row['ACTIVIDAD'],lista_actividades),
          axis=1, result_type='expand').T.values

sin_fumigada = Tools.run_sum_reset(full_seguimiento['dias_sin_fumigada'],1)
sin_siembra = Tools.run_sum_reset(full_seguimiento['dias_sin_siembra'],1)
sin_renovada = Tools.run_sum_reset(full_seguimiento['dias_sin_renovada'],1)
sin_desbrosada = Tools.run_sum_reset(full_seguimiento['dias_sin_desbrosada'],1)
sin_abonada = Tools.run_sum_reset(full_seguimiento['dias_sin_abonada'],1)
sin_enmienda = Tools.run_sum_reset(full_seguimiento['dias_sin_enmienda'],1)
sin_rotobo = Tools.run_sum_reset(full_seguimiento['dias_sin_rotobo'],1)

full_seguimiento.drop(columns=['dias_sin_fumigada', 'dias_sin_siembra', 'dias_sin_renovada','dias_sin_desbrosada',
                               'dias_sin_abonada','dias_sin_enmienda','dias_sin_rotobo'], inplace=True)
full_seguimiento = pd.concat([full_seguimiento,sin_fumigada,sin_siembra,sin_renovada,sin_desbrosada,sin_abonada,sin_enmienda,sin_rotobo], axis=1)
#full_seguimiento.drop(columns=['lote_change','actividad_change'], inplace=True)
del(sin_fumigada,sin_siembra,sin_renovada,sin_desbrosada,sin_abonada,sin_enmienda,sin_rotobo)
###############################################333

'''
5- splits para pastoreo y otras actividades
'''

#split dataframe hato: traer antiguedad de pasto (n-1), filtrar solo PASTOREO, order HATO, date
full_seguimiento = Data_Process.lag_hatos(full_seguimiento, destination)

### SUBIR DATOS DE ACTIVIDADES LOTES A LA DB
#df_lotes_acti = seguimiento[seguimiento['ACTIVIDAD']!='PASTOREO']
df_lotes_acti = otras_acti[['lote_id','FECHA ACTIVIDAD', 'ACTIVIDAD', 'PRODUCTO']]
df_lotes_acti.sort_values(by=['FECHA ACTIVIDAD'], inplace=True)

map_actividades = {'FUMIGADA':1, 'ABONADA':2, 'RENOVADA':3, 'SIEMBRA':4, 'FOLIAR':5, 'ENMIENDA':6,
                   'ENCALADA':7, 'ENCALADO':7, 'DESBROSADA':8, 'CORTE':9, 'HENOLAJE':10, 'INTERSIEMBRA':11,
                   'RASTRILLO':12, 'RIEGO':13, 'ROTOVO':14, 'ROTOBO':14}

#df_lotes_acti = pd.merge(df_lotes_acti, lotes.iloc[:,-2:], how='left', left_on='lote_c', right_on='nombre_lote')
df_lotes_acti['ACTIVIDAD'] = df_lotes_acti['ACTIVIDAD'].map(map_actividades).fillna(df_lotes_acti['ACTIVIDAD'])

upload_df_acti = df_lotes_acti.copy()
upload_df_acti['ID_ACT_LOTE']=0; upload_df_acti['ID_OPERARIO']=34; 
upload_df_acti = upload_df_acti[['ID_ACT_LOTE','lote_id','FECHA ACTIVIDAD', 'ACTIVIDAD', 'PRODUCTO', 'ID_OPERARIO']]
upload_df_acti.columns = ['ID_ACT_LOTE', 'ID_LOTE', 'FECHA_ACTIVIDAD', 'ID_Tipo_Actividad', 'Producto', 'ID_OPERARIO']
upload_df_acti.dropna(subset = ["ID_LOTE"], inplace=True)

''' UPLOAD TO SQL '''

#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.185.0.199" + "/" + "lv_test")

upload_df_acti.to_sql('Actividades_Lotes', engine,  if_exists='append', index=False)

### SUBIR DATOS DE PASTO CULTIVADO A SQL
map_variedad_lote = {"KYKUYO":1, "RAYGRAS":2}
upload_pasto = pasto_actual.copy()
upload_pasto = upload_pasto[['lote_id','TIPO DE FORRAJE']]
upload_pasto['ID_variedad'] = upload_pasto['TIPO DE FORRAJE'].map(map_variedad_lote).fillna(upload_pasto['TIPO DE FORRAJE'])
upload_pasto = upload_pasto[['lote_id','ID_variedad']]
upload_pasto.columns = ['ID_LOTE', 'ID_variedad']
upload_pasto['ID_LOTE'] = upload_pasto['ID_LOTE'].astype(int)

## read SQL lotes para actualizar asi el cultivo
### cargue de lotes
'''
sql = "select * from lv_test.lotes"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
lotes_sql = pd.read_sql(sql, engine)
lotes_sql = pd.merge(lotes_sql.iloc[:,:-1], upload_pasto, how='left', on='ID_LOTE')
lotes_sql.to_sql('lotes', engine,  if_exists='replace', index=False)
'''

###use of API to patch
#to dictionary
data_dict = upload_pasto.to_dict(orient='records')
#loop each row, remove nan and patch API
for data_row in data_dict:
    to_del=[]
    for k,v in data_row.items():
        if type(v) == float and math.isnan(v):
            #print(k,'delete')
            to_del.append(k)
    for n in to_del:
        data_row.pop(n)
    #API to upload each dict
    API_usage.patch_lote(token,data_row,data_row['ID_LOTE'])
#se podria hacer mas rapido si el patch aceptara una lista, y el ID no se enviara en el header
    
### Dataframe de pastoreo = ubicacion vacas + traslado vacas historicos
df_pastoreo = seguimiento[seguimiento['ACTIVIDAD']=='PASTOREO']
df_leche_hato = df_pastoreo[['HATO','FECHA ACTIVIDAD', 'NUMERO ANIMALES', 'LECHE TOTAL']]
df_pastoreo = df_pastoreo[['lote_c','FECHA ACTIVIDAD','HATO']]


### Dataframe Leche por hato
df_leche_hato = df_leche_hato[df_leche_hato['LECHE TOTAL']>0 ]
df_leche_hato['ID_HATO'] = np.where(df_leche_hato['HATO'] == 'ISLA', 5, 
                                    np.where(df_leche_hato['HATO'] == 'JUNCAL', 4, 
                                             np.where(df_leche_hato['HATO'] == 'PARAISO', 5, 
                                                      np.where(df_leche_hato['HATO'] == 'PROXIMAS JUN', 4, 
                                                               np.where(df_leche_hato['HATO'] == 'RECODO 1', 1, 
                                                                        np.where(df_leche_hato['HATO'] == 'RECODO 2', 2, 
                                                                                 np.where(df_leche_hato['HATO'] == 'RECODO 3', 3, df_leche_hato['HATO'])))))))

#group by fecha, id_hato = sum animales y sum leche total
df_grouped = df_leche_hato.groupby(['ID_HATO','FECHA ACTIVIDAD']).agg(
     Numero_Animales= ('NUMERO ANIMALES','sum'),
     Leche_Total = ('LECHE TOTAL','sum'),
     ).reset_index()
df_grouped['ID_OPERARIO'] = 35
df_grouped.rename(columns={"FECHA ACTIVIDAD": "FECHA_ACTIVIDAD"}, inplace=True)
#Upload to SQL
df_grouped.to_sql('Leche_Hatos', engine,  if_exists='append', index=False)
#transform HATO to ID_HATO, lote_c to ID_LOTE

#upload to SQL [ID_TRASLADO_HATO, Fecha_Traslado, ID_HATO, ID_LOTE]
'''
lag_lotes.csv se sube al Dash, para animaciones
a lag lotes, se le podria agregar el planeamiento futuro, para visualizar todo
de full_seguimiento, generar alertas automaticas, por venicmiento de fechas de actividades-lotes y enviar a tabla de SQL de alertas, o correo, o App o todas
loes "T", no tienen nunca pastoreo ni actividades. en tabla Lotes, poner variable para ver si se debe monitorear o es distinto


'''
