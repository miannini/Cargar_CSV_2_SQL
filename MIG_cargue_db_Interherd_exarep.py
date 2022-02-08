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
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

#query = 
'''SELECT distinct pd.ID IDdiagpre, pd.Animal ID_VACA, pd.Result ID_Resultado, pd.Days Dias, pd.Service ID_servicio,
    pd.OpID ID_Actividad
    FROM [RECODO].[dbo].[PregDiags] pd;'''
  
 
#exapre = pd.read_sql(query, cnxn)

query2 = '''SELECT distinct op.ID ID_Actividad, op.Animal ID_VACA, op.OperationType ID_TipoOperacion, 
    op.Result ID_Resultado, case op.Officer
             when 3 then 33
             when 7 then 25
             when 12 then 36
             when 13 then 9
             when 15 then 9
             when 17 then 7
             when 19 then 2
             when 20 then 23
             else 35 end ID_OPERARIO, 1 ID_Categoria, op.Date Fecha, op.Remarks Comentario, NULL Fecha_programada 
    FROM [RECODO].[dbo].[Operations] op
    WHERE op.[OperationType] in (18,53,73,5,107,135,123,66,21,19,40,29,89,100,136,72,88,90,43,55,65,85) AND op.[Done]=1;'''
  
act_exarep = pd.read_sql(query2, cnxn)


### cargue de vacas para comparar
sql = "select ID_VACA, Nombre_Vaca from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

vacas_exarep = pd.DataFrame(list(set(act_exarep['ID_VACA'])), columns=['ID_VACA'])

#aqui explorar si alguna no tiene nombre o falta de alguna forma en la DB
vacas_compare = pd.merge(vacas_exarep,vacas, how="left", on='ID_VACA')

#upload datos de actividades partos
act_exarep_to_upload = act_exarep.drop(['ID_Actividad','Fecha_programada'], axis=1)
#corregir resultado con mapeo M4A
map_results = pd.read_csv("D:/M4A/DB_RECODO/mapeo_resultados_ExaRep.csv")
map_actividades = pd.read_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_TipoOper.csv")
act_exarep_to_upload = pd.merge(act_exarep_to_upload, map_results.loc[:,['Result','ID_resut_M4A']], how='left', left_on='ID_Resultado', right_on='Result')
act_exarep_to_upload = pd.merge(act_exarep_to_upload, map_actividades.loc[:,['OperationType','ID_m4a']], how='left', left_on='ID_TipoOperacion', right_on='OperationType')

act_exarep_to_upload['ID_Resultado'] = act_exarep_to_upload['ID_resut_M4A']
act_exarep_to_upload['ID_Resultado'] = act_exarep_to_upload['ID_Resultado'].replace(np.nan, 34)
act_exarep_to_upload['ID_TipoOperacion'] = act_exarep_to_upload['ID_m4a']

act_exarep_to_upload.drop(['Result','ID_resut_M4A','OperationType','ID_m4a'], axis=1, inplace=True)
act_exarep_to_upload.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)
opers = list(set(act_exarep_to_upload['ID_TipoOperacion']))


#obtener ID de actividad para enlazar con tabla de Partos
sql = '''SELECT * FROM lv_test.Actividades_Vacas
        WHERE ID_TipoOperacion in (8,11,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33)'''
actDB = pd.read_sql(sql, engine)

#actualizar actividades local, para agregar ID de DB real
act_exarepDB = pd.concat([actDB.loc[:,['ID_Actividad']],act_exarep], 1)
act_exarepDB.set_axis(['ID_Actividad','Old_ID_Actividad','ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria','Fecha','Comentario','Fecha2'], axis='columns',inplace=True)

#exarep_to_upload = pd.merge(exarep,act_exarepDB.loc[:,['ID_Actividad','Old_ID_Actividad']],how="left",left_on="ID_Actividad",right_on="Old_ID_Actividad")

'''
no trae datos de ID_TipoOperacion 101 = Diagnostico prenez 5 meses
parece que esos datos no estan en la tabla [RECODO].[dbo].[PregDiags]
'''
