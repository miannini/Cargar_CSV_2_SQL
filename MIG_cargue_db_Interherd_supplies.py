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
### suministros
query = '''SELECT distinct su.ID IDsuministro
	 , su.Code Code
	 , su.Name Nombre
    , su.Unit Unidad
    , su.DefaultValue Valor_Default
    , su.Sire Sire
    , su.Supplier Proveedor
    , su.Discontinued Fecha_descontinuado
  FROM [RECODO].[dbo].[Supplies] su;'''
supply = pd.read_sql(query, cnxn)
supply.replace(r'^\s*$', np.nan, regex=True, inplace=True)
### proveedores
query2 = '''SELECT distinct su.ID IDproveedor
	 , su.Code Code
	 , su.Name Nombre
    , su.Tel Telefono
    , su.Email email
    , su.Contact contacto
    , su.PostalCorr PostCode
    , su.Discontinued Fecha_descontinuado
  FROM [RECODO].[dbo].[Suppliers] su;'''
supplier = pd.read_sql(query2, cnxn)
supplier.replace(r'^\s*$', np.nan, regex=True, inplace=True)
### tipo-suministros
query3 = '''SELECT distinct su.ID IDClase_sumi
	 , su.Code Code
	 , su.Name Nombre
    , su.Semen Semen
    , su.InMEdReg Medicina
    , su.Feed Alimento
    , su.Discontinued Fecha_descontinuado
  FROM [RECODO].[dbo].[SupplyClasses] su;'''
supply_classes = pd.read_sql(query3, cnxn)
supply_classes.replace(r'^\s*$', np.nan, regex=True, inplace=True)
### mapa suministros-tipo de clase
query4 = '''SELECT distinct su.ID ID
	 , su.Supply ID_Suministro
	 , su.Class ID_Clase
    , su.Discontinued Fecha_descontinuado
  FROM [RECODO].[dbo].[SuppSuCl] su;'''
map_clas_sup = pd.read_sql(query4, cnxn)
map_clas_sup.replace(r'^\s*$', np.nan, regex=True, inplace=True)
### origen de suministros
query5 = '''SELECT distinct su.ID ID
	 , su.Supply ID_Suministro
	 , su.Supplier ID_proveedor
    , sl.Units Unidades
    , sl.UnitCost Costo_unitaro
    , sl.OrigDate Fecha_ingreso
    , sl.Discontinued1 Fecha_descontinuado
    , sl.WorkBal Balance
  FROM [RECODO].[dbo].[SuppLotsOrig] su
  INNER JOIN [RECODO].[dbo].[SuppLots] sl ON su.ID=sl.OrigLot'''
sup_origen = pd.read_sql(query5, cnxn)
sup_origen['ID_proveedor'].replace(0, np.nan, regex=False, inplace=True)
sup_origen['Costo_unitaro'].replace(0, np.nan, regex=False, inplace=True)
sup_origen['fecha_vencimiento'] = np.nan
sup_origen['numero_lote'] = np.nan
sup_origen['numero_factura'] = np.nan
sup_origen['presentacion'] = np.nan
#reorder
#agregar fecha_vencimiento, numero_lote, numero_factura, presentacion (tipo unidad)

### uso de suministros
query6 = '''SELECT distinct su.ID ID
	 , su.Lot ID_Lote_origen
	 , su.Supply ID_suministro
    , su.Units Unidades
    , su.DestType Tipo_destino
    , su.Date Fecha
    , su.UnitCost Costo_unitaro
    , su.Animal ID_VACA
    , su.Event ID_ACTIVIDAD
  FROM [RECODO].[dbo].[SuppLotsDest] su'''
sup_destino = pd.read_sql(query6, cnxn)
#mapeo de actividades desde CSVs
servicios = pd.read_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_servicios.csv")
diag_pre = pd.read_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_diagpre.csv")
partos = pd.read_csv("D:/M4A/DB_RECODO/mapeo_actividades_vacas_partos.csv")
#falta operation_type 53, 73, 78, 18, purgas, vacunas, etc
lista_cols = ['ID_VACA','ID_Actividad_x','ID_Actividad_y']
actividades_map = pd.concat([servicios.loc[:,lista_cols], diag_pre.loc[:,lista_cols], partos.loc[:,lista_cols]], ignore_index=True)

sup_destino_upload = pd.merge(sup_destino, actividades_map, how="left",left_on="ID_ACTIVIDAD",right_on="ID_Actividad_x")
sup_destino_upload.drop(['ID_VACA_y','ID_Actividad_x'], axis=1, inplace=True)
sup_destino_upload.rename({"ID_VACA_x":"ID_VACA"}, axis='columns', inplace=True)
sup_destino_upload["ID_ACTIVIDAD"] = sup_destino_upload["ID_Actividad_y"]
sup_destino_upload.drop(["ID_Actividad_y"], axis=1, inplace=True)
######################################################
### subir a database MySQL
#engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv")
engine = sa.create_engine("mysql+pymysql://" + "m4a.DA" + ":" + "m4a2020" + "@" + "35.229.36.251" + "/" + "lv_test")

supply.to_sql('Suministros', engine,  if_exists='append', index=False)
supplier.to_sql('Proveedores', engine,  if_exists='append', index=False)
supply_classes.to_sql('Tipo_suministros', engine,  if_exists='append', index=False)
map_clas_sup.to_sql('Map_clases_suministros', engine,  if_exists='append', index=False)
sup_origen.to_sql('Suministros_origen', engine,  if_exists='append', index=False)
sup_destino_upload.to_sql('Suministros_destino', engine,  if_exists='append', index=False)

'''
* Suministros= 
suministro, con valor y moneda (ej algunas cosas en USD, COP u otro)
tabla de unidades unica, para asociar con Suministros[Unidad] (opcional)
volumen en columna separada
columna para propios(pajillas de toros propios, abono, otros)


* Tipo-suministros=
code agregar mas cosas, para lotes, tratamientos, concetrnado, limpieza, administracion, etc

* proveedores = 
NIT, ciudad, deptto, tipo-suministros

* tipo operaciones = 
agregar columna de categoria de operacion
agregar columna de categoria de suministros
agregar columna de suministro especifico (si aplica alguno)


* crear uso de stock =
id_lote, actividad_lote, tipo destino

*crear tabla tipo-destino = 
id, code, nombre

* crear monitoreo stock por suministro
sumatoria por tipo de suministro, suministro y total unidades disponibles (- sumatoria de uso de stock)




'''