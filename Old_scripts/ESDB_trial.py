import os
import pandas as pd
import numpy as np
import unidecode
import pyodbc

### EDB
import pyesedb
help(pyesedb)
file_object = open("RECODO.edb", "rb") #../DB_interherd/
esedb_file = pyesedb.file()
esedb_file.open_file_object(file_object)

esedb_file.check_file_signature_file_object(file_object)
esedb_file.close()

####tabla Vacas###########
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=..\DB_interherd;Database=RECODO;Trusted_Connection=yes;')

query = '''select [ID] ID_VACA
	  ,1 ID_CLIENTE
	  ,0 ID_FINCA
	  ,[ElectronicID]
	  ,[Name] Nombre_Vaca
	  ,[Breed]-101 Raza
	  ,[Sex] Sexo
	  ,[DamB] VacaMadre
	  ,[DamParity] IDparto
      ,[OrigDate] FechaRegistro
      ,CASE 
		WHEN [OrigType] <= 0 THEN [OrigType]+2
		WHEN [OrigType] > 0 THEN [OrigType]+1
		END IDTipoOrigen
	  ,[BirthDate] FechaNacimiento
	  ,CASE 
		WHEN [Fate] <= 0 THEN [Fate]+2
		WHEN [Fate] > 0 THEN [Fate]+1
		END IDTipoSalida
      ,[FateDate] FechaSalida
      ,[Sire]
  FROM [RECODO].[dbo].[Animals]'''
  
#      ,1 Estado
#      ,1 Estado_Final
vacas = pd.read_sql(query, cnxn)

vacas_excel = pd.read_csv('../DB_interherd/todos_los_datos.csv')
cols = ['ID', 'Raza', 'Sexo/ tipo', 'Padre' , 'Madre gen.','Madre ', 'Nacimiento', 'Nombre', 'Id. Electr?ica', 'Toro ?t. parto']
vacas_excel = vacas_excel[cols]
#vacas_excel.ID_VACA = vacas_excel.ID_VACA.map(lambda x: 1 if x==0 else x)
#vacas_excel['Id. Electr?ica'] = vacas_excel['Id. Electr?ica'].map(lambda x: None if x == '' else x)
#vacas_excel.Raza = vacas_excel.Raza.map(lambda x: 0 if x == -101 else x)
#vacas_excel.VacaMadre = vacas_excel.VacaMadre.map(lambda x: 1 if x == 0 else x)
''' no se usa
import mysql.connector

db_connection = mysql.connector.connect(
  	host="34.73.96.30",
  	user="m4a.DA",
  	passwd="m4a2020"
    )
'''
import sqlalchemy as sa
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "34.73.96.30" + "/" + "lv")

vacas.to_sql('vacas', engine,  if_exists='append', index=False)

sql = "select * from lv.vacas"
vacasM4A = pd.read_sql(sql, engine)


######

#Mastitis
#este archivo viene de DropBox, excel de mastitis
mastitis = pd.read_csv('D:\M4A\SQLDB\Dash\data\Historico_Mastitis_Inversiones_Camacho-Vacas.csv')

mastitis.AI = mastitis.AI.replace('P','5').fillna('0').astype(int)
mastitis.AD = mastitis.AD.replace('P','5').fillna('0').astype(int)
mastitis.PI = mastitis.PI.replace({'P':'5', 'PI' : '0'}).fillna('0').astype(int)
mastitis.PD = mastitis.PD.replace('P','5').fillna('0').astype(int)

mastitis.columns = ['ID_VACA', 'NOMBRE', 'HATO', 'AI', 'AD', 'PI', 'PD', 'ORDEÑADOR',
       'Comentario', 'Fecha', 'Ubre_sana', 'Calificacion', 'GAP',
       'Chequeo_revision']

mastitis['ID_OPERARIO'] = 0
mastitis.loc[mastitis.ORDEÑADOR == 'ANDRES',['ID_OPERARIO']] = 19
mastitis.loc[mastitis.ORDEÑADOR == 'MARTHA',['ID_OPERARIO']] = 28
mastitis.loc[mastitis.ORDEÑADOR == 'NESTOR',['ID_OPERARIO']] = 25
mastitis.loc[mastitis.ORDEÑADOR == 'ANCELMO',['ID_OPERARIO']] = 29
mastitis.loc[mastitis.ORDEÑADOR == 'FERNANDO',['ID_OPERARIO']] = 23
mastitis.loc[mastitis.ORDEÑADOR == 'OSCAR',['ID_OPERARIO']] = 9
mastitis.loc[mastitis.ORDEÑADOR == 'JHON',['ID_OPERARIO']] = 13
mastitis.loc[mastitis.ORDEÑADOR == 'JUAN',['ID_OPERARIO']] = 34
mastitis.loc[mastitis.ID_OPERARIO == 0,'ID_OPERARIO'] = 35

mastitis['ID_TipoOperacion'] = 9
mastitis['ID_Resultado'] = 34
mastitis['ID_Categoria'] = 1

mastitis_actividad = mastitis.loc[:,['ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria', 'Fecha', 'Comentario']]

import sqlalchemy as sa
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")

mastitis_actividad.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)
sql = "select * from lv.Actividades_Vacas"
actDB = pd.read_sql(sql, engine)

mastitisDB = pd.concat([actDB.loc[:,['ID_Actividad']],mastitis.loc[:,['AI', 'AD', 'PI', 'PD','Chequeo_revision' , 'Ubre_sana', 'Calificacion', 'GAP']] ], 1)
mastitisDB.to_sql('Mastitis', engine,  if_exists='append', index=False)



###############
##Traslado vacas
traslados_excel = pd.read_csv('../DB_interherd/traslado_vacas_V2.csv')

traslados_excel.Grupo.replace({'NI':'9', 'CF':'8', 'JCAL': '4', 'R 2':'2', 'R 1':'1', 'R 3':'3', 'PARAI':'5', 'HOR.1':'6', 'HO.JU': '7'}, inplace = True)
traslados_excel.Grupo = traslados_excel.Grupo.astype(int)
traslados_excel = traslados_excel[1:]
traslados_excel.ID = traslados_excel.ID.str.strip()
traslados_excel = traslados_excel.drop_duplicates()

import sqlalchemy as sa
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")
sql = "select ID_VACA, Nombre_Vaca from lv.vacas where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)

traslados = pd.merge(traslados_excel,vacas, how='left', left_on = 'ID', right_on = 'Nombre_Vaca', indicator = True)

traslados['duplicados'] = traslados.groupby(['ID'])['ID'].transform('size')
traslados['Fecha_Traslado'] = pd.to_datetime("2021-04-27", format='%Y-%m-%d', errors='coerce')
traslados_ready = traslados.loc[(traslados.duplicados < 2) & (traslados._merge != 'left_only'),:]
traslados_ready = traslados_ready.loc[:,['Fecha_Traslado','ID_VACA','Grupo']]
traslados_ready.columns = ['Fecha_Traslado','ID_VACA','ID_HATO']
traslados_ready.drop_duplicates(inplace=True)
traslados_ready.drop_duplicates(subset='ID_VACA', keep="last", inplace = True)

traslados_ready.to_sql('Traslado_Vacas', engine,  if_exists='append', index=False)

traslados2 = traslados_ready.loc[:,['ID_VACA','ID_HATO']]
traslados2['ID_LOTE'] = 9999
traslados2.to_sql('Ubicacion_Vacas', engine,  if_exists='replace', index=False)

############################################
#missing vacas sin ID en DB
vacas_missing = traslados[~traslados['ID_VACA'].notnull()]
vacas_missing.reset_index(drop=True, inplace = True)
vacas_excel['ID'] = vacas_excel['ID'].str.strip()

vacas_missing2 = pd.merge(vacas_missing, vacas_excel, how='left', left_on='ID', right_on='ID')
vacas_missing2['Nombre'].fillna(vacas_missing2['ID'], inplace=True) 
#remove not required columns
#transform raza, sex, format date, id electronica a float no e+
vacas_missing2.drop(['Grupo','ID','Fecha_Traslado','ID_VACA','Nombre_Vaca','duplicados','_merge'], axis=1, inplace=True)
vacas_missing2['Madre '].fillna(vacas_missing2['Madre gen.'], inplace=True) 
vacas_missing2.rename(columns={'Sexo/ tipo': 'Sexo', 'Madre ': 'VacaMadre', 'Id. Electr?ica':'ElectronicID', 'Nacimiento':'FechaNacimiento', 'Padre':'Sire', 'Nombre':'Nombre_Vaca', }, inplace=True)
vacas_missing2['ID_CLIENTE']=1
vacas_missing2['IDTipoOrigen']=2
vacas_missing2['IDTipoSalida']=2
vacas_missing2['FechaRegistro']=pd.to_datetime("2021-04-25", format='%Y-%m-%d', errors='coerce')
vacas_missing2['FechaNacimiento']=pd.to_datetime(vacas_missing2['FechaNacimiento'], format='%d/%m/%Y', errors='coerce')
vacas_missing2.Raza.replace({'AN':'6', 'AY':'18', 'GY(50%) x HO(50%)': '3', 'HO':'2', 'HO(50%) x JE(50%)':'2', 'HO(75%) x GY(25%)':'2', 'HO(75%) x JE(25%)':'2','HO(88%) x GY(13%)':'2','HO(88%) x JE(13%)':'2' ,'HO(92%) x JE(8%)':'2', '??': '17'}, inplace = True)
vacas_missing2['Sexo'].replace({'He:Rp':'1'}, inplace = True)

#reorder columns
cols = ['ID_CLIENTE', 'ElectronicID','Nombre_Vaca', 'Raza', 'Sexo', 'VacaMadre', 'FechaRegistro', 'IDTipoOrigen', 'FechaNacimiento', 'IDTipoSalida', 'Sire'] # 'IDparto','FechaSalida',
vacas_missing2.drop(['Madre gen.','Toro ?t. parto'], axis=1, inplace=True)
vacas_missing2 = vacas_missing2.reindex(columns=cols)


###################################################
# 
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")
sql = "select ID_VACA, Nombre_Vaca from lv.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
vacas = pd.read_sql(sql, engine)
#join 

#2 filtro - vacas en DB SQL
vacas_missing2 =  pd.merge(vacas_missing2, vacas, how='left', left_on='Nombre_Vaca', right_on='Nombre_Vaca')
vacas_missing2 = vacas_missing2[vacas_missing2['ID_VACA'].isnull() ]
vacas_missing2.drop(['ID_VACA'], axis=1, inplace=True)
#union con madres
vacas_missing2 = pd.merge(vacas_missing2, vacas, how='left', left_on='VacaMadre', right_on='Nombre_Vaca')
vacas_missing2['VacaMadre'] = vacas_missing2['ID_VACA']
vacas_missing2.drop(['ID_VACA','Nombre_Vaca_y'], axis=1, inplace=True)
vacas_missing2.rename(columns={'Nombre_Vaca_x': 'Nombre_Vaca'}, inplace=True)
vacas_missing2 = vacas_missing2.drop_duplicates(['Nombre_Vaca'], keep='last')

vacas_missing2.to_sql('vacas', engine,  if_exists='append', index=False)


#just to compare in case required
sql = "select * from lv.vacas"#" where IDTipoSalida = 2 or IDTipoSalida = 1"
full_vacas = pd.read_sql(sql, engine)


##############################################
#duplicated analysis
duplicados = traslados.loc[(traslados.duplicados >1),:]
duplicados = duplicados.reindex(columns=['ID_VACA','Nombre_Vaca'])
duplicados.drop_duplicates(subset=['ID_VACA','Nombre_Vaca'], keep="last",inplace=True)
duplicados = pd.merge(duplicados, full_vacas, how='left', left_on='ID_VACA', right_on='ID_VACA')
duplicados.to_csv('duplicados.csv')

razas = set(vacas_excel['Raza'])
