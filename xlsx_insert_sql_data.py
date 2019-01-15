#!/usr/bin/env python
# coding: utf-8

#### import module ####
import pandas as pd
import pymssql
import sys
import collections as cs

# Connection string / windows authentication
cnxn = pymssql.connect(server='Emeatristsql', database='Data_Proses2')

def create_table(mylist,table_name):
    
    script = ("create table _{} ( \n".format(table_name))
    for i in mylist:
        a1 = ("[{}] [nvarchar](4000) NULL,\n".format(i))
        script += a1
    script += ")\n"

    script = script.replace("NULL,\n)", "NULL\n)")
    print(script)
    return(script)

def list_comparison(file_path, table_name):

    df = pd.read_csv(file_path, nrows=1, sep="|", encoding="cp1254")

    script = ("""
    select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{}'
    ORDER BY ORDINAL_POSITION ASC
    """.format(table_name))

    dframe = pd.read_sql(script, cnxn)

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
        print("Excel data ile sql tablosunun kolon sıralarını kontrol ediniz!")
        print(a)
        print(b)
        # TODO: Bu kısma kolon sırası farklı ise kolonları exceldeki sıraya göre sıralayıp yazacak bir yapı eklenecek.
        # create_table fonksiyonu ile temp tablo tüm özellikleri ile korunup kullanılıp sonrasında silinecek.

    else:
        print("result = 0")
        print("Kolonlar uyumlu :)")

    


# file_path = r"\\Emeatristsql\IMP\LSCS1540\Benim_Aksigortam_YUKLE_3.csv"
# table_name = "LSCS1540_IKAME_KONUT_TEMP"
# table_name = "LSCS1540_BENIM_AKSIGORTAM_TEMP"

# usage : python .\test2.py ".\trdt.csv" tablename

if __name__ == "__main__":

    file_path = str(sys.argv[1])
    table_name = str(sys.argv[2])
    list_comparison(file_path, table_name)
