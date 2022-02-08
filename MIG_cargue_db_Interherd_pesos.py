# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 19:52:00 2021

@author: ASUS-PC
"""


import os
import pandas as pd
import numpy as np
import unidecode
import pyodbc
import sqlalchemy as sa

####tabla Vacas###########
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=LAPTOP-OVDQCMQI\SQLEXPRESS;Database=RECODO;Trusted_Connection=yes;') #Server=.\DESCORCIA

query = '''SELECT distinct pe.ID IDpeso, pe.Animal ID_VACA, pe.Weight Peso, pe.OpID ID_Actividad 
        FROM [RECODO].[dbo].[Weights] pe;'''
  
 
pesos = pd.read_sql(query, cnxn)

query2 = '''SELECT distinct op.ID ID_Actividad, op.Animal ID_VACA, 10 ID_TipoOperacion, 1 ID_Resultado, 35 ID_OPERARIO,
        1 ID_Categoria, op.Date Fecha, op.Remarks Comentario, NULL Fecha_programada
        FROM [RECODO].[dbo].[Operations] op
        WHERE op.[OperationType] in (23) AND op.[Done]=1;''' #15 is parto, 90 lactoinduccion
  
act_pesos = pd.read_sql(query2, cnxn)


#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

### cargue de vacas para comparar
sql = "select ID_VACA, Nombre_Vaca from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

vacas_pesos = pd.DataFrame(list(set(act_pesos['ID_VACA'])), columns=['ID_VACA'])

#aqui explorar si alguna no tiene nombre o falta de alguna forma en la DB
vacas_compare = pd.merge(vacas_pesos,vacas, how="left", on='ID_VACA')

#upload datos de actividades partos
act_pesos_to_upload = act_pesos.drop(['ID_Actividad','Fecha_programada'], axis=1)
act_pesos_to_upload.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

#obtener ID de actividad para enlazar con tabla de Partos
sql = '''SELECT * FROM lv_test.Actividades_Vacas
        WHERE ID_TipoOperacion=10'''
actDB = pd.read_sql(sql, engine)

#actualizar actividades local, para agregar ID de DB real
act_pesosDB = pd.concat([actDB.loc[:,['ID_Actividad']],act_pesos], 1)
act_pesosDB.set_axis(['ID_Actividad','Old_ID_Actividad','ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria','Fecha','Comentario','Fecha2'], axis='columns',inplace=True)

pesos_to_upload = pd.merge(pesos,act_pesosDB.loc[:,['ID_Actividad','Old_ID_Actividad']],how="left",left_on="ID_Actividad",right_on="Old_ID_Actividad")
#save a copy to future uses
pesos_to_upload.to_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_pesos.csv")
#reorder columns
pesos_to_upload.rename({"ID_Actividad_y":"ID_ACTIVIDAD"}, axis='columns', inplace=True)
pesos_to_upload.drop(['ID_Actividad_x','Old_ID_Actividad'], axis=1, inplace=True)

#actualizar tabla de partos y upload a DB
pesos_to_upload.to_sql('Pesos', engine,  if_exists='append', index=False)


### cargue de vacas para comparar ID_parto
#sql = "select * from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
#vacas = pd.read_sql(sql, engine)