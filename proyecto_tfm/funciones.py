# coding=utf-8

# Autor: x273062

# Fecha: 2021/11/02

import subprocess

import pyspark.sql.functions as fn

from pyspark import StorageLevel

from pyspark.sql import SQLContext, DataFrame

from pyspark.sql.types import DateType

from datetime import datetime, timedelta

import getpass

 

 

class clase_func_comunes:

    

    fecha_activa = "9999-12-31"

    fecha_activa_2 = "0001-01-01"

    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    fecha_anio = datetime.now().year

    fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Guardar en cache dataframes

    memory_disk_offheap = StorageLevel(True, True, True, False, 1)

    memory_and_disk = StorageLevel(True, True, False, False, 1)

    memory = StorageLevel(False, True, False, False, 1)

    user = getpass.getuser()


    def __init__(self) -> None:
        pass

    def cruce_tablas(self, df1: DataFrame, df2: DataFrame, clave_cruce: list,campos_df1, campos_df2 = None, tipo_cruce: int = 1):

        """

        Función para realizar los cruces a mi manera

        Tipo cruce:

            -1 devuelve solo el inner

            -2 devuelve el inner y el left anti

            -3 devuelve el inner, el left anti y el right anti

            -4 devuelve el inner y el right anti

 

        :param df1:

        :param df2:

        :param clave_cruce:

        :param tipo_cruce: int, default = 1 Must be one of: 1,2,3, and 4.

       :return: type: DataFrame

        """

 

        #chequeo de valores de entrada

        if tipo_cruce < 1 or tipo_cruce > 7:

            raise ValueError("Tipo cruce no valido: 1 - Inner, 2 - Inner + left , 3 - Inner + left + right, 4 - Inner + right, 5 - left anti , 6 - right anti, 7 - left")

 

        if campos_df1 == "*":

            campos_df1 = df1.columns

 

        if campos_df2 == "*":

            campos_df2 = df2.columns

 

        # realizamos el cruce

 

        if campos_df2 is None:

            cruzan = df1.alias("a").join(df2.alias("b"), on=clave_cruce, how="inner").select([fn.col("a." + n) for n in campos_df1])

            solof1_left = df1.alias("a").join(df2.alias("b"), on=clave_cruce, how="left").select([fn.col("a." + n) for n in campos_df1])

        else:

            cruzan = df1.alias("a").join(df2.alias("b"), on=clave_cruce, how="inner").select([fn.col("a." + n) for n in campos_df1] + [fn.col("b." + x) for x in campos_df2])

            solof1_left= df1.alias("a").join(df2.alias("b"), on=clave_cruce, how="left").select([fn.col("a." + n) for n in campos_df1] + [fn.col("b." + x) for x in campos_df2])

        solof1 = df1.join(df2, on=clave_cruce, how="left_anti")

        solof2 = df2.join(df1, on=clave_cruce, how="left_anti")

 

        if tipo_cruce == 1:

            return cruzan

        elif tipo_cruce == 2:

            return cruzan, solof1

        elif tipo_cruce == 3:

            return cruzan, solof1, solof2

        elif tipo_cruce == 4:

            return cruzan, solof2

        elif tipo_cruce == 5:

            return solof1

        elif tipo_cruce == 6:

            return solof2

        elif tipo_cruce == 7:

            return solof1_left

 

 


    def renombrar_campos(self, df:DataFrame, cols_ren : list = None):

 

        if cols_ren is None:

           cols_rename = df.columns

        else:

            cols_rename = cols_ren

 

        for cols in cols_rename:

            df = df.withColumnRenamed(cols, cols + "_n")

        return df

  

