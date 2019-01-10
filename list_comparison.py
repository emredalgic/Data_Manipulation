#!/usr/bin/env python
# coding: utf-8

#### import module ####
import operator
import csv
import pandas as pd
import pymssql
from sqlalchemy import create_engine
import sys
import collections as cs
from pandas.io import sql

import pyodbc
import time
# Connection string / windows authentication
# cnxn = pymssql.connect(server='Emeatristsql',database='Data_Proses2', autocommit=True)
# mydb = create_engine('mssql+pymssql://Emeatristsql/{}'.format("Data_Proses2"))

cnxnx = pyodbc.connect('DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={};DATABASE={};UID={};PWD={};'.format(
    "Emeatristsql", "Data_Proses2", "lsmuretim", "lsmuretim09"))


def standart_insert(mylist,file_path,file_name,target_table):
    
    # First step write schema.ini file

    file = open("{}schema.ini".format(file_path), "w")
    # print("[{}{}]".format(file_path, file_name))
    file.write("[{}]\n".format(file_name))
    file.write("ColNameHeader=True\n")
    file.write("Format=Delimited(|)\n")
    file.write("CharacterSet=ANSI\n")
    # file.write('DateTimeFormat = "yyyy.mm.dd"\n')
    file.close()

    script=("""
   
    IF EXISTS (select * from dbo.sysobjects where iD = Object_id (N'[dbo].{}') and OBJECTPROPERTY(id, N'IsUserTable') = 1)
    Delete t1 from {} t1 (nolock)

    declare @Query varchar(max),
            @localFilePath varchar(4000)=N'{}',
            @Dosya varchar(4000)=replace('{}','.csv','#csv'),
            @kolonlar varchar(max)=N'{}',
            @target_table varchar(200)=N'{}'

    SET @Query = 
            '
            Insert  Into '+ @target_table +'
            ( ' + @kolonlar + ' )
            SELECT ' + @kolonlar + '  FROM  OPENDATASOURCE(''Microsoft.ACE.OLEDB.12.0'',''Data Source = ' + @localFilePath + ';Extended Properties =Text'')...'+convert(varchar (1000),@Dosya)+'
            '
            --print @Query
            Exec(@Query)
    """.format(target_table, target_table, file_path, file_name, mylist, target_table))

    print(script)
    return(script)

def list_comparison(file_path, table_name):

    df = pd.read_csv(file_path, nrows=1, sep="|", encoding="cp1254")

    script = ("""
    select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{}'
    ORDER BY ORDINAL_POSITION ASC
    """.format(table_name))

    dframe = pd.read_sql(script, cnxnx)

    a = df.columns.tolist()  # csv list
    b = dframe["COLUMN_NAME"].tolist()  # sql table

    diff1 = list(set(a) - set(b)) # Exceldeki değişimler + eklentiler
    diff2 = list(set(b) - set(a)) # Tablodaki değişimler + eklentiler

    kont = (a) == (b) # iki listenin kolon sıraları aynı mı

    # print(kont)
    # print(list(set(a).intersection(set(b))))

    if len(diff1) > 0 and len(diff2) > 0 :
        print("Excel template dosyasi ile {} adli sql tablosu kolonlari esit sayida degil !".format(
            table_name))
        print("Exceldeki Eslesmeyen kolonlar :", diff1)
        print("Tablodaki Eslesmeyen kolonlar :", diff2)
    elif len(diff1) == 0 and len(diff2) > 0 :
        print("Sql tablosundaki ({}) eslesmeyen kolonlar :".format(table_name),diff2)
    
    elif len(diff1) > 0 and len(diff2) == 0 :
        print("Exceldeki({}) eslesmeyen kolonlar :".format(table_name),diff1)

    elif len(diff1) == 0 and len(diff2) == 0 and kont == False:
        print("Excel data ile sql tablosunun kolon sıraları aynı değil,Bulk insert insert yerine OPENDATASOURCE ile insert işlemi kolon kolona yapılacak.")
        print("Bu yöntem Bulk Insert 'e göre çok daha yavaş olduğundan uzun sürmesi halinde kolon sırasını uyumlu hale getirip tekrar deneyiniz.")
        
        file_name = file_path.split(u'\\')[-1]

        filepath = file_path.replace(file_name, "")

        mylist = ','.join("[{0}]".format(x) for x in a)

        tsql_script = standart_insert(mylist,filepath,file_name,table_name)
        cnxnx.autocommit = True
        cursor = cnxnx.cursor()
        cursor.execute(tsql_script)

        # time.sleep(50)

        cursor.close()
        cnxnx.close()
        print(pyodbc.drivers())
        print("Process Complete.")


    else:
        print("result = 0")
        print("Kolonlar uyumlu :)")
   


# usage : python c:/Users/emre.dalgic/Dropbox/Apps/Python/List_Comparison/list_comparison.py "\\Emeatristsql\IMP\LSCS1540\Benim_Aksigortam_YUKLE_4.csv" LSCS1540_BENIM_AKSIGORTAM_TEMP

if __name__ == "__main__":

    file_path = str(sys.argv[1])
    table_name = str(sys.argv[2])
    list_comparison(file_path, table_name)
