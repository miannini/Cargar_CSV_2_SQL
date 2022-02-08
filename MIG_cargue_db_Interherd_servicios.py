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
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=LAPTOP-OVDQCMQI\SQLEXPRESS;Database=RECODO;Trusted_Connection=yes;')

query = '''SELECT distinct se.ID IDservicio
	 , se.Animal ID_VACA
	 , se.Sire Sire
    , se.EmbryoDonor ID_Embrion
    , se.OpID ID_Actividad 
  FROM [RECODO].[dbo].[Services] se;'''
  
 
servicios = pd.read_sql(query, cnxn)

query2 = '''SELECT distinct op.ID ID_Actividad
	 , op.Animal ID_VACA
     , case op.OperationType
     when 17 then 2
     when 56 then 12
     else 2 end ID_TipoOperacion
     , case op.OperationType
     when 17 then 2
     when 56 then 1
     else 2 end ID_Resultado
     , case op.Officer
     when 3 then 33
     when 7 then 25
     when 12 then 36
     when 13 then 9
     when 15 then 9
     when 17 then 7
     when 19 then 2
     when 20 then 23
     else 35 end ID_OPERARIO
     , 12 ID_Categoria 
     , op.Date Fecha
     , op.Remarks Comentario
     , NULL Fecha_programada 
  FROM [RECODO].[dbo].[Operations] op
  WHERE op.[OperationType] in (17, 56) AND op.[Done]=1;''' #17 is srvc, 56 implante embrion , 2 ID_TipoOperacion
  
act_servicios = pd.read_sql(query2, cnxn)


#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

### cargue de vacas para comparar
sql = "select ID_VACA, Nombre_Vaca from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

vacas_servicios = pd.DataFrame(list(set(act_servicios['ID_VACA'])), columns=['ID_VACA'])

#aqui explorar si alguna no tiene nombre o falta de alguna forma en la DB
vacas_compare = pd.merge(vacas_servicios,vacas, how="left", on='ID_VACA')

#upload datos de actividades partos
act_servicios_to_upload = act_servicios.drop(['ID_Actividad','Fecha_programada'], axis=1)
#corregir actividad inseminacion o toro
act_servicios_to_upload['ID_Categoria'] = np.where(act_servicios_to_upload['Comentario'].str.contains('(?i)insemi', regex=True),12,
                                                   np.where(act_servicios_to_upload['Comentario'].str.contains('(?i)toro', regex=True),13,12))


act_servicios_to_upload.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

#obtener ID de actividad para enlazar con tabla de Partos
sql = '''SELECT * FROM lv_test.Actividades_Vacas
        WHERE ID_TipoOperacion in (2,12)'''
actDB = pd.read_sql(sql, engine)

#actualizar actividades local, para agregar ID de DB real
act_serviciosDB = pd.concat([actDB.loc[:,['ID_Actividad']],act_servicios], 1)
act_serviciosDB.set_axis(['ID_Actividad','Old_ID_Actividad','ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO',
                          'ID_Categoria','Fecha','Comentario','Fecha2'], axis='columns',inplace=True)

servicios_to_upload = pd.merge(servicios,act_serviciosDB.loc[:,['ID_Actividad','Old_ID_Actividad']],
                               how="left",left_on="ID_Actividad",right_on="Old_ID_Actividad")
#save a copy to future uses
servicios_to_upload.to_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_servicios.csv")
#reorder columns
servicios_to_upload.rename({"ID_Actividad_y":"ID_ACTIVIDAD"}, axis='columns', inplace=True)
servicios_to_upload.drop(['ID_Actividad_x','Old_ID_Actividad'], axis=1, inplace=True)

#actualizar tabla de partos y upload a DB
servicios_to_upload.to_sql('Servicios', engine,  if_exists='append', index=False)


### cargue de vacas para comparar ID_parto
#sql = "select * from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
#vacas = pd.read_sql(sql, engine)