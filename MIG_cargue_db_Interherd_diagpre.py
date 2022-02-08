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

query = '''SELECT distinct pd.ID IDdiagpre, pd.Animal ID_VACA, pd.Result ID_Resultado, pd.Days Dias, 
        pd.Service ID_servicio, pd.OpID ID_Actividad
        FROM [RECODO].[dbo].[PregDiags] pd;'''
  
 
diagpre = pd.read_sql(query, cnxn)

query2 = '''SELECT distinct op.ID ID_Actividad, op.Animal ID_VACA, 
            case op.OperationType
                when 91 then 7
                when 92 then 4
                when 93 then 5
                when 96 then 3
                when 101 then 3
                when 102 then 5
                else 6 end ID_TipoOperacion,
            op.Result ID_Resultado, 
            case op.Officer
                when 3 then 33
                when 7 then 25
                when 12 then 36
                when 13 then 9
                when 15 then 9
                when 17 then 7
                when 19 then 2
                when 20 then 23
                else 35 end ID_OPERARIO, 18 ID_Categoria, op.Date Fecha, op.Remarks Comentario, NULL Fecha_programada 
        FROM [RECODO].[dbo].[Operations] op
        WHERE op.[OperationType] in (91,92,93,96,101,102,106) AND op.[Done]=1;'''
  
act_diapre = pd.read_sql(query2, cnxn)


#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

### cargue de vacas para comparar
sql = "select ID_VACA, Nombre_Vaca from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

vacas_diapre = pd.DataFrame(list(set(act_diapre['ID_VACA'])), columns=['ID_VACA'])

#aqui explorar si alguna no tiene nombre o falta de alguna forma en la DB
vacas_compare = pd.merge(vacas_diapre,vacas, how="left", on='ID_VACA')

#upload datos de actividades partos
act_diapre_to_upload = act_diapre.drop(['ID_Actividad','Fecha_programada'], axis=1)
#corregir resultado con mapeo M4A
map_results = pd.read_csv("D:/M4A/DB_RECODO/mapeo_resultados_DiagPre.csv")
act_diapre_to_upload = pd.merge(act_diapre_to_upload, map_results.loc[:,['ID','id_resultado_M4A']], how='left', left_on='ID_Resultado', right_on='ID')

act_diapre_to_upload['ID_Resultado'] = act_diapre_to_upload['id_resultado_M4A']
act_diapre_to_upload['ID_Resultado'] = act_diapre_to_upload['ID_Resultado'].replace(np.nan, 34)
act_diapre_to_upload.drop(['ID','id_resultado_M4A'], axis=1, inplace=True)

act_diapre_to_upload.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

#obtener ID de actividad para enlazar con tabla de Partos
sql = '''SELECT * FROM lv_test.Actividades_Vacas
        WHERE ID_TipoOperacion in (3,4,5,6,7)'''
actDB = pd.read_sql(sql, engine)

#actualizar actividades local, para agregar ID de DB real
act_diapreDB = pd.concat([actDB.loc[:,['ID_Actividad']],act_diapre], 1)
act_diapreDB.set_axis(['ID_Actividad','Old_ID_Actividad','ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria','Fecha','Comentario','Fecha2'], axis='columns',inplace=True)

diagpre_to_upload = pd.merge(diagpre,act_diapreDB.loc[:,['ID_Actividad','Old_ID_Actividad']],how="left",left_on="ID_Actividad",right_on="Old_ID_Actividad")

'''
no trae datos de ID_TipoOperacion 101 = Diagnostico prenez 5 meses
parece que esos datos no estan en la tabla [RECODO].[dbo].[PregDiags]
'''

#save a copy to future uses
diagpre_to_upload.to_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_diagpre.csv")
#reorder columns
diagpre_to_upload.rename({"ID_Actividad_y":"ID_ACTIVIDAD"}, axis='columns', inplace=True)
diagpre_to_upload.drop(['ID_Actividad_x','Old_ID_Actividad'], axis=1, inplace=True)

#corregir referencia de servicios
#map_services = pd.read_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_servicios.csv")
#diagpre_to_upload = pd.merge(diagpre_to_upload, map_services.loc[:,['ID_servicio','ID_Actividad_y']], how='left', left_on='ID_servicio', right_on='ID_Actividad_x')
#act_diapre_to_upload['ID_servicio'] = act_diapre_to_upload['id_resultado_M4A']

#actualizar tabla de partos y upload a DB
diagpre_to_upload.to_sql('DiagPre', engine,  if_exists='append', index=False)


### cargue de vacas para comparar ID_parto
#sql = "select * from lv_test.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
#vacas = pd.read_sql(sql, engine)