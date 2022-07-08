# coding=utf-8


import subprocess

import pyspark.sql.functions as fn

from pyspark import StorageLevel

from pyspark.sql import SQLContext, DataFrame

from pyspark.sql.types import DateType

from datetime import datetime, timedelta

import getpass

 

 

class clase_func_comunes:

 

    lista_cols_host = ["data_cons_timestamp", "data_date_part", "data_date_part_origen", "data_timestamp_part_origen"]

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


 

    def check_duplicados(self, campos: list , df: DataFrame):

        """

        Realiza el groupby y devuelve un False si no hay duplicados o True si los hay

        :param df:

        :param campos:

        :return:

 

        duplicados = df.groupby(campos).count().where('count > 1')  # type: DataFrame

        if duplicados.rdd.isEmpty() != True:

            print("Cuidado hay duplicados")

            return True

        else:

            return False

        """

        return False

 

    def borrar_columnas(self,df: DataFrame, cols : list):

        for col in cols:

            df = df.drop(fn.col(col))

        return  df

 

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

 

 

    def cruce_bsprodcom(self, df1, bsprodcom):

        """

        Cruzamos un df contra la tabla bsprodcom para recuperar la familia que se nos pide

        :param df1:

        :param df2:

        :return:

        """

 

        pk_cruce = ["bscodcia", "bsramoco", "bsprocom"]

        cruzan = self.cruce_tablas(df1 = df1,df2 = bsprodcom, clave_cruce=pk_cruce, campos_df1= "*", campos_df2=None, tipo_cruce=1)

        return cruzan

 

    def renombrar_campos(self, df:DataFrame, cols_ren : list = None):

 

        if cols_ren is None:

           cols_rename = df.columns

        else:

            cols_rename = cols_ren

 

        for cols in cols_rename:

            df = df.withColumnRenamed(cols, cols + "_n")

        return df

 

    def transformar_timestamp(self,df:DataFrame, campo : str):

        """

        Esta función transforma la columna timestamp en una nueva de tipo DateType

        """

 

        df = df.withColumn(f"{campo}_tra", fn.substring(fn.col(campo),1,10))

        return df

 

    def f_mueve_hdfsAlocal(self,origen, final):

        """

        Mueve un fichero distribuido HDFS al filesystem del servidor remoto

        :param origen: str.  Ruta de origen

        :param final: str.  Ruta de destino

        :return: Nada

        """

        import subprocess

        subprocess.call(['hdfs', 'dfs', '-copyToLocal','-f', origen, final])

 

    def genera_csv(self,sdf, archivo_final, sep=';', header=True):

        import os

        import numpy as np

        prefix = str(np.random.randint(1000))

        try:

            sdf.repartition(1).write.csv("./" + prefix + "tmp.csv", sep=sep, header=header)

            self.f_mueve_hdfsAlocal(origen="./" + prefix + "tmp.csv", final=prefix + 'tmp.csv')

            os.system(" mv ./" + prefix + "tmp.csv/*.csv " + archivo_final)

            os.system(" rm -R ./" + prefix + "tmp.csv")

            os.system(" hdfs dfs -rm -R ./" + prefix + "tmp.csv")

        except Exception as e:

            print(e)

            os.system(" rm -R ./" + prefix + "tmp.csv")

            os.system(" hdfs dfs -rm -R ./" + prefix + "tmp.csv")

 

    def flatten_df(self,nested_df : DataFrame):

        """

        Función cogida de microsoft recibe un dataframe y hace un flatten a las columnas que tienen datos nested

        """

        stack = [((), nested_df)]

        columns = []

 

        while len(stack) > 0:

            parents, df = stack.pop()

 

            flat_cols = [

                fn.col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))

                for c in df.dtypes

                if c[1][:6] != "struct"

            ]

 

            nested_cols = [

                c[0]

                for c in df.dtypes

                if c[1][:6] == "struct"

            ]

 

            columns.extend(flat_cols)

 

            for nested_col in nested_cols:

                projected_df = df.select(nested_col + ".*")

                stack.append((parents + (nested_col,), projected_df))

 

        return nested_df.select(columns)

 

    def subidaTabla(self,spark: SQLContext, dir_nodo: str, dir_hdfs: str, bbdd: str, nombre_archivo: str,nombre_tabla : str,

                    carpeta_hdfs: str = None):

 

        # primero pasamos del local al hdfs

        dir_local = dir_nodo + nombre_archivo

        resultado = False

        try:

            subprocess.call(['hdfs', 'dfs', '-copyFromLocal', '-f', dir_local, dir_hdfs])

        except Exception as e:

            print(e)

 

        try:

            if carpeta_hdfs is None:

                carpeta_hdfs = ""

 

            url_nodo = carpeta_hdfs + nombre_archivo

            fichero_spark = spark.read.csv(url_nodo, sep=";", header=True)

 

        except Exception as e:

            print(e)

            print("no se ha podido leer")

 

        try:

            url_subida = f"{bbdd}.{nombre_tabla}"

            fichero_spark.coalesce(1).write.saveAsTable(url_subida, mode='overwrite')

            print(f"El fichero {nombre_archivo} se ha subido a la bbdd : {bbdd}")

            resultado = True

        except Exception as e:

            print("no se ha podido  subir")

            print(e)

            resultado = False

 

        return resultado

 

    def cargaFichero(self,spark: SQLContext, dir_nodo: str, dir_hdfs: str, nombre_archivo: str,

                        carpeta_hdfs: str = None) -> DataFrame:

        """

        Cargamos en HDFS un fichero y nos devuelve un df que no se ha subido a tabla

        """

 

        # primero pasamos del local al hdfs

        dir_local = dir_nodo + nombre_archivo

        try:

            subprocess.call(['hdfs', 'dfs', '-copyFromLocal', '-f', dir_local, dir_hdfs])

        except Exception as e:

            print(e)

 

        try:

            if carpeta_hdfs is None:

                carpeta_hdfs = ""

 

            url_nodo = carpeta_hdfs + nombre_archivo

            fichero_spark = spark.read.csv(url_nodo, sep=";", header=True)

 

        except Exception as e:

            print(e)

            print("no se ha podido leer")

 

 

        return fichero_spark

 

    def borrar_papelera_hive(self):

        """

        Vaciamos la papelera de Hive

        """

        try:

            print("Vaciando papelera hive user...")

            subprocess.run(['hdfs', 'dfs', '-rm', '-r', f'hdfs://nameservice1/user/{self.user}/.Trash/*'],

                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            print("...Papelera vacía")

        except Exception:

            print("Error al vaciar la papelera de hive...")

 

        try:

            print("Vaciando papelera hive seg_prestaciones...")

            subprocess.run(['hdfs', 'dfs', '-rm', '-r', 'hdfs://nameservice1/project/seg_prestaciones_ops/.Trash/*'],

                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            print("...Papelera vacía")

        except Exception:

            print("Error al vaciar la papelera de hive...")