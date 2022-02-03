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
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

def actividades_vacas():
    query = '''SELECT distinct op.ID ID_OP_IH, op.Animal ID_VACA, case op.OperationType when 96  then 3 when 101  then 3
            when 92 then 4 when 93  then 5 when 102  then 5 when 104 then 6 when 106 then 6 when 91 then 7  when 18 then 8
            when 94 then 8 else 0 end ID_TipoOperacion, coalesce(op.Result,34) ID_RESUL_IH, 36 ID_OPERARIO, op.Date Fecha, 
            Op.Remarks Comentario
            FROM [RECODO].[dbo].[Operations] op
            WHERE op.[OperationType] in (96, 101, 92, 93,102, 104,106, 91, 18,94);'''

    operac = pd.read_sql(query, cnxn)
    mapeo = pd.read_csv('D:/M4A/SQLDB/Mapeo_tipoOperacion_Interherd_mysql.csv', sep = ";")
    mapeo.rename(columns = {'ID_RESULT_M4A' : 'ID_Resultado', 'ID_CATEGORIA_M4A' : 'ID_Categoria'}, inplace = True)

    operac = pd.merge(operac, mapeo.loc[:,['ID_RESUL_IH', 'ID_Resultado', 'ID_Categoria']].drop_duplicates(), how = 'left', on = 'ID_RESUL_IH')
    operac=operac.loc[:,['ID_VACA', 'ID_TipoOperacion', 'ID_Resultado', 'ID_OPERARIO', 'ID_Categoria','Fecha', 'Comentario']]

    operac.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

    return print('actividades_vacas uploaded')