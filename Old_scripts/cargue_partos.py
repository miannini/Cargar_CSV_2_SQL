
import os
import pandas as pd
import numpy as np
import unidecode
import pyodbc
import sqlalchemy as sa

####tabla Vacas###########
cnxn = pyodbc.connect(r'Driver=SQL Server;Server=.\DESCORCIA;Database=RECODO;Trusted_Connection=yes;')
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

def partos():
    query = '''select oper.ID ID_OP, oper.ANIMAL ID_VACA, 1 ID_TipoOperacion, 1 ID_Resultado, 36 ID_OPERARIO, 
            oper.Date Fecha, oper.Remarks Comentario, par.Animal VACA, par.Number NumeroDePartos, Par.ID ID_PARTO
            FROM dbo.Operations oper
            full join dbo.Parities as par on (oper.ID = par.OpPar)
            where oper.OperationType = 15'''

    partos = pd.read_sql(query, cnxn)
    partos.drop_duplicates(inplace = True)

    partos.ID_OP.isna().value_counts()
    partos.ID_PARTO.isna().value_counts()
    partos['dif'] = partos.ID_VACA - partos.VACA
    partos.dif.value_counts()
    partos.Comentario.value_counts()

    listOfStrings = ['H', 'M', 'H.MUERT', 'M.MUERT', 'ABORTO', 'PRETÉRMINO', 'PRETERMINO', 'DISTOCIA', 'GEMELOS',
                     'RETENCIÓN', 'RETENCION', 'LACTOINDUCCIÓN', 'LACTOINDUCCION']


    partos['CATEGORIA'] = partos.Comentario.map(lambda x: x.upper()  if x.upper() in listOfStrings else "No definido")

    partos['ID_Categoria'] = partos.CATEGORIA.replace({"No definido": "1", "M": "3", "H": "2", "M.MUERT": "4",
                                                       "H.MUERT": "5", "ABORTO": "8", "PRETÉRMINO": "6",
                                                       "LACTOINDUCCION": "11", "GEMELOS": "10",
                                                       "PRETERMINO": "6"}).astype(int)

    partos.CATEGORIA.value_counts()
    partos_M4A = partos.loc[:,['ID_VACA','ID_TipoOperacion', 'ID_Resultado', 'ID_OPERARIO', 'ID_Categoria', 'Fecha', 'Comentario']]
    partos_M4A.to_sql('Actividades_Vacas', engine,  if_exists='append', index=False)

    sql = "select * from lv.Actividades_Vacas where ID_TipoOperacion = 1"
    actDB = pd.read_sql(sql, engine)

    partos['ID_Actividad'] = actDB['ID_Actividad']

    ### Leer vacas de mysql
    sql = "select ID_VACA VACA_HIJA, IDparto ID_PARTO from lv.vacas where IDparto is not null"
    vacas = pd.read_sql(sql, engine)
    vacas = pd.merge(vacas,partos, how = 'left', on = ['ID_PARTO'])

    partos.loc[:,['ID_Actividad', 'NumeroDePartos']].to_sql('Actividad_Partos', engine,  if_exists='append', index=False)

    vacas.to_csv("Mapeo_vacas_partos_intherherd_m4a.csv", sep = ";", index = False)
    partos.to_csv("Mapeo_partos_intherherd_m4a.csv", sep = ";", index = False)

    vacasM4A = vacas.loc[:,['VACA_HIJA', 'ID_Actividad']]
    vacasM4A.columns = ['ID_VACA', 'IDparto']
    vacasM4A = vacasM4A.loc[vacasM4A.IDparto.isna() == False,:].reset_index()

    #vacasM4A = vacasM4A.loc[vacasM4A.ID_VACA >= 4063,:]

    for i in range(len(vacasM4A)):
      sql = "UPDATE vacas SET IDparto = " + vacasM4A.IDparto[i].astype(str)+" WHERE ID_VACA = " + vacasM4A.ID_VACA[i].astype(str)
      engine.execute(sql)


    vacasM4A = vacas.loc[:,['VACA_HIJA', 'ID_Actividad']]
    vacasM4A.columns = ['ID_VACA', 'IDparto']
    vacasM4A = vacasM4A.loc[vacasM4A.IDparto.isna() == True,:].reset_index()

    #vacasM4A = vacasM4A.loc[vacasM4A.ID_VACA >= 4063,:]

    for i in range(len(vacasM4A)):
      sql = "UPDATE vacas SET IDparto = NULL WHERE ID_VACA = " + vacasM4A.ID_VACA[i].astype(str)
      engine.execute(sql)

    return print('partos uploaded')
