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

query = '''SELECT distinct pa.ID IDparto
	 , pa.Animal ID_VACA
	 , pa.Number Numero_Parto
     , pa.Sire Sire
     , pa.OpPar ID_Actividad
     , NULL Asistida   
  FROM [RECODO].[dbo].[Parities] pa;'''
  
 
partos = pd.read_sql(query, cnxn)

query2 = '''SELECT distinct op.ID ID_Actividad
	 , op.Animal ID_VACA
     , 1 ID_TipoOperacion
     , 1 ID_Resultado
     , 35 ID_OPERARIO
     , case op.OperationType
     when 15 then 1
     when 90 then 11
     else 0 end ID_Categoria
     , op.Date Fecha
     , op.Remarks Comentario
     , NULL Fecha_programada 
  FROM [RECODO].[dbo].[Operations] op
  WHERE op.[OperationType] in (15, 90) AND op.[Done]=1;''' #15 is parto, 90 lactoinduccion
  
act_partos = pd.read_sql(query2, cnxn)


#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

### cargue de vacas para comparar
sql = "select ID_VACA, Nombre_Vaca from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

vacas_partos = pd.DataFrame(list(set(act_partos['ID_VACA'])), columns=['ID_VACA'])

#aqui explorar si alguna no tiene nombre o falta de alguna forma en la DB
vacas_compare = pd.merge(vacas_partos,vacas, how="left", on='ID_VACA')

#upload datos de actividades partos
act_partos_to_upload = act_partos.drop(['ID_Actividad','Fecha_programada'], axis=1)
act_partos_to_upload['actividad_corr'] = np.where(act_partos_to_upload['Comentario'].str.contains('M.MUERT', regex=False),4,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)macho muer', regex=True),4,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('H.MUERT', regex=False),5,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('InterHerd', regex=False),1,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)abor', regex=True),8,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)pret', regex=True),6,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)disto', regex=True),7,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)geme', regex=True),10,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)rete', regex=True),9,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)lacto', regex=True),11,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('H', regex=False),2,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)hembr', regex=True),2,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('(?i)macho', regex=True),3,
                                                  np.where(act_partos_to_upload['Comentario'].str.contains('M', regex=False),3,1))))))))))))))
act_partos_to_upload['ID_Categoria'] = np.where(act_partos_to_upload['ID_Categoria'] == 1, act_partos_to_upload['actividad_corr'], act_partos_to_upload['ID_Categoria'])
act_partos_to_upload.drop(['actividad_corr'], axis=1, inplace=True)
act_partos_to_upload['ID_Actividad']=0
cols = act_partos_to_upload.columns.tolist()
cols = cols[-1:] + cols[:-1]
act_partos_to_upload = act_partos_to_upload[cols] 
act_partos_to_upload.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

#obtener ID de actividad para enlazar con tabla de Partos
sql = '''SELECT * FROM lv_test.Actividades_Vacas
        WHERE ID_TipoOperacion=1'''
actDB = pd.read_sql(sql, engine)

#actualizar actividades local, para agregar ID de DB real
act_partosDB = pd.concat([actDB.loc[:,['ID_Actividad']],act_partos], 1)
act_partosDB.set_axis(['ID_Actividad','Old_ID_Actividad','ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria','Fecha','Comentario','Fecha2'], axis='columns',inplace=True)

partos_to_upload = pd.merge(partos,act_partosDB.loc[:,['ID_Actividad','Old_ID_Actividad']],how="left",left_on="ID_Actividad",right_on="Old_ID_Actividad")
#save a copy to future uses
partos_to_upload.to_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_partos.csv")
#reorder columns
cols = partos_to_upload.columns.tolist()
cols = cols[:4] + cols[-2:-1] + cols[5:6] + cols[4:5]+ cols[-1:]
partos_to_upload = partos_to_upload[cols] 
partos_to_upload.rename({"ID_Actividad_y":"ID_ACTIVIDAD"}, axis='columns', inplace=True)
partos_to_upload.drop(['Asistida','ID_Actividad_x','Old_ID_Actividad'], axis=1, inplace=True)

#actualizar tabla de partos y upload a DB
partos_to_upload.to_sql('Partos', engine,  if_exists='append', index=False)


### cargue de vacas para comparar ID_parto
#sql = "select * from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
#vacas = pd.read_sql(sql, engine)