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

query = '''SELECT distinct si.ID IDsire, si.Active Active, si.IDOfficial IDOfficial, si.AINumber AINumber, 
        si.LongName Nombre_Largo, si.Registration Registro, si.Breed Raza, si.Discontinued Fecha_descontinuado,
        FROM [RECODO].[dbo].[Sires] si;'''
  
 
sire = pd.read_sql(query, cnxn)


#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

#corregir razas con mapeo M4A
map_razas = pd.read_csv("D:/M4A/DB_RECODO/mapa_razas_csv.csv")

sire_to_upload = pd.merge(sire, map_razas.loc[:,['ID','ID_M4A']], how='left', left_on='Raza', right_on='ID')

sire_to_upload['Raza'] = sire_to_upload['ID_M4A']
#replace empto to nan
sire_to_upload.replace(r'^\s*$', np.nan, regex=True, inplace=True)

sire_to_upload.drop(['ID','ID_M4A'], axis=1, inplace=True)
sire_to_upload.to_sql('Sires', engine,  if_exists='append', index=False)