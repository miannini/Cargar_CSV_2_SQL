import os
import pandas as pd
import numpy as np
import unidecode
import pyodbc
import mysql.connector
import sqlalchemy as sa


####tabla Vacas###########
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.\DESCORCIA;Database=RECODO;Trusted_Connection=yes;')

db_connection = mysql.connector.connect(host="34.73.96.30", user="m4a.DA", passwd="m4a2020")

engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "34.73.96.30" + "/" + "lv")
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

def vacas():
    query = '''select [ID] ID_VACA, 1 ID_CLIENTE, 0 ID_FINCA, [ElectronicID], [Name] Nombre_Vaca, [Breed]-101 Raza, [Sex] Sexo, 
            [DamB] VacaMadre, [DamParity] IDparto, [OrigDate] FechaRegistro, 
            CASE WHEN [OrigType] <= 0 THEN [OrigType]+2 WHEN [OrigType] > 0 THEN [OrigType]+1 END IDTipoOrigen, 
            [BirthDate] FechaNacimiento, CASE WHEN [Fate] <= 0 THEN [Fate]+2 WHEN [Fate] > 0 THEN [Fate]+1 END IDTipoSalida, 
            [FateDate] FechaSalida, [Sire], 1 Estado, 1 Estado_Final  
            FROM [RECODO].[dbo].[Animals]'''

    vacas = pd.read_sql(query, cnxn)

    vacas.ID_VACA = vacas.ID_VACA.map(lambda x: 1 if x==0 else x)
    vacas.ElectronicID = vacas.ElectronicID.map(lambda x: None if x == '' else x)
    vacas.Raza = vacas.Raza.map(lambda x: 0 if x == -101 else x)
    vacas.VacaMadre = vacas.VacaMadre.map(lambda x: 1 if x == 0 else x)

    vacas.to_sql('vacas', engine,  if_exists='append', index=False)

    sql = "select * from lv.vacas"
    vacasM4A = pd.read_sql(sql, engine)
    return print('Mastitis_uploadded')

######

#Mastitis
def mastitis():
    mastitis = pd.read_csv('D:\M4A\SQLDB\Dash\data\Historico_Mastitis_Inversiones_Camacho-Vacas.csv')

    mastitis.AI = mastitis.AI.replace('P', '5').fillna('0').astype(int)
    mastitis.AD = mastitis.AD.replace('P', '5').fillna('0').astype(int)
    mastitis.PI = mastitis.PI.replace({'P': '5', 'PI': '0'}).fillna('0').astype(int)
    mastitis.PD = mastitis.PD.replace('P', '5').fillna('0').astype(int)

    mastitis.columns = ['ID_VACA', 'NOMBRE', 'HATO', 'AI', 'AD', 'PI', 'PD', 'ORDEÑADOR',
           'Comentario', 'Fecha', 'Ubre_sana', 'Calificacion', 'GAP',
           'Chequeo_revision']

    mastitis['ID_OPERARIO'] = 0
    mastitis.loc[mastitis.ORDEÑADOR == 'ANDRES', ['ID_OPERARIO']] = 19
    mastitis.loc[mastitis.ORDEÑADOR == 'MARTHA', ['ID_OPERARIO']] = 28
    mastitis.loc[mastitis.ORDEÑADOR == 'NESTOR', ['ID_OPERARIO']] = 25
    mastitis.loc[mastitis.ORDEÑADOR == 'ANCELMO', ['ID_OPERARIO']] = 29
    mastitis.loc[mastitis.ORDEÑADOR == 'FERNANDO', ['ID_OPERARIO']] = 23
    mastitis.loc[mastitis.ORDEÑADOR == 'OSCAR', ['ID_OPERARIO']] = 9
    mastitis.loc[mastitis.ORDEÑADOR == 'JHON', ['ID_OPERARIO']] = 13
    mastitis.loc[mastitis.ORDEÑADOR == 'JUAN', ['ID_OPERARIO']] = 34
    mastitis.loc[mastitis.ID_OPERARIO == 0, 'ID_OPERARIO'] = 35

    mastitis['ID_TipoOperacion'] = 9
    mastitis['ID_Resultado'] = 34
    mastitis['ID_Categoria'] = 1

    mastitis_actividad = mastitis.loc[:,['ID_VACA','ID_TipoOperacion','ID_Resultado','ID_OPERARIO','ID_Categoria', 'Fecha', 'Comentario']]

    mastitis_actividad.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)
    sql = "select * from lv.Actividades_Vacas"
    actDB = pd.read_sql(sql, engine)

    mastitisDB = pd.concat([actDB.loc[:,['ID_Actividad']],mastitis.loc[:,['AI', 'AD', 'PI', 'PD','Chequeo_revision' , 'Ubre_sana', 'Calificacion', 'GAP']] ], 1)
    mastitisDB.to_sql('Mastitis', engine,  if_exists='append', index=False)
    return print('Mastitis_uploadded')



###############
##Traslado vacas
def traslados():
    traslados_excel = pd.read_csv('D:/M4A/SQLDB/Dash/data/traslado_vacas_V1.csv')

    traslados_excel.Grupo.replace({'NI':'9', 'CF':'8', 'JCAL': '4', 'R 2':'2', 'R 1':'1', 'R 3':'3', 'PARAI':'5', 'HOR.1':'6', 'HO.JU': '7'}, inplace = True)
    traslados_excel.Grupo = traslados_excel.Grupo.astype(int)
    traslados_excel = traslados_excel[1:]
    traslados_excel.ID = traslados_excel.ID.str.strip()
    traslados_excel = traslados_excel.drop_duplicates()

    sql = "select ID_VACA, Nombre_Vaca from lv.vacas where IDTipoSalida = 2 or IDTipoSalida = 1"
    vacas = pd.read_sql(sql, engine)

    traslados = pd.merge(traslados_excel,vacas, how='left', left_on = 'ID', right_on = 'Nombre_Vaca', indicator = True)

    traslados['duplicados'] = traslados.groupby(['ID'])['ID'].transform('size')
    traslados['Fecha_Traslado'] = pd.to_datetime("2020-12-19", format='%Y-%m-%d', errors='coerce')
    traslados = traslados.loc[(traslados.duplicados < 2) & (traslados._merge != 'left_only'),:]
    traslados = traslados.loc[:,['Fecha_Traslado','ID_VACA','Grupo']]
    traslados.columns = ['Fecha_Traslado','ID_VACA','ID_HATO']
    traslados.drop_duplicates(inplace=True)
    traslados.drop_duplicates(subset='ID_VACA', keep="last", inplace = True)

    traslados.to_sql('Traslado_Vacas', engine,  if_exists='append', index=False)

    traslados2 = traslados.loc[:,['ID_VACA','ID_HATO']]
    traslados2['ID_LOTE'] = 9999
    traslados2.to_sql('Ubicacion_Vacas', engine,  if_exists='append', index=False)
    return print('Traslados_uploaded')
