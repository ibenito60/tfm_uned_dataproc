from calendar import c
from typing_extensions import Self
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as fn
from soupsieve import select
from pyspark.sql.types import DateType
from datetime import datetime, timedelta
import getpass
from pyspark import StorageLevel


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
        """
        Renombra las columnas del dataframe
        """
        if cols_ren is None:
           cols_rename = df.columns
        else:
            cols_rename = cols_ren
        for cols in cols_rename:
            df = df.withColumnRenamed(cols, cols + "_n")
        return df


class ingesta_datos():

    def __init__(self, spark: SparkSession) -> None:
        self.fc = fc.clase_func_comunes()
        self.spark = spark
        self.polizas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/polizas.txt",inferSchema=True, sep=',', header=True)
        self.siniestros = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/siniestros.txt",inferSchema=True, sep=',', header=True)
        self.pagos = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/pagos_siniestro.txt",inferSchema=True, sep=',', header=True)
        self.recibos = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/recibos.txt",inferSchema=True, sep=',', header=True)
        self.clientes = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/clientes.txt",inferSchema=True, sep=',', header=True)
        self.intervinientes = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/intervinientes.txt",inferSchema=True, sep=',', header=True)
        self.cuentas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/cuentas.txt",inferSchema=True, sep=',', header=True)
        self.tarifas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/tarifas.txt",inferSchema=True, sep=',', header=True)
    
    def siniestros_pagos(self)-> DataFrame:
        """
        Construimos los siniestros y los pagos de los siniestros
        """
        pagos_siniestros = self.pagos.withColumnRenamed('estado_pago','estado')
        siniestros = self.siniestros
        #agrupamos los siniestros
        pagos_siniestros_struct = pagos_siniestros.withColumn("datos_pago", fn.struct([col for col in pagos_siniestros.columns if col not in ["idsiniestro"]]))
        pagos_agg = pagos_siniestros_struct.groupBy("idsiniestro").agg(fn.collect_list("datos_pago").alias("datos_pago"))

        print(pagos_agg.printSchema())

        #realizamos el cruce entre los pagos y los siniestros por siniestro
        cruzan_001 = self.fc.cruce_tablas(df1=siniestros, df2=pagos_agg, clave_cruce=['idsiniestro'], campos_df1="*",
            campos_df2=["datos_pago"], tipo_cruce=7)
        
        #realizo la agrupacion de los siniestros
        siniestros_struct = cruzan_001.withColumn("datos_sin", fn.struct([col for col in cruzan_001.columns if col not in ["idpoliza"]]))
        siniestros_agg = siniestros_struct.groupBy("idpoliza").agg(fn.collect_list("datos_sin").alias("datos_sin"))
        #cargamos el fichero 
        #siniestros_struct.coalesce(1).write.parquet("gs://...siniestros", mode='overwrite')
        return siniestros_agg
    
    def recibos_tra(self)-> DataFrame:
        """
        Realizamos la agrupación de los recibos.
        Devuelve un dataframe
        """

        recibos = self.recibos.withColumn("datos_recibos", fn.struct([col for col in self.recibos.columns if col not in ['idpoliza']]))
        #agrupamos por poliza
        recibos_agg = recibos.groupBy("idpoliza").agg(fn.collect_list("datos_recibos").alias("datos_recibos"))
        return recibos_agg

    def tarifas_tra(self)-> DataFrame:
        """
        Realizamos la agrupación de las tarifas.
        Devuelve un dataframe.
        """

        tarifas = self.tarifas.withColumn("datos_tarifa", fn.struct([col for col in self.tarifas.columns if col not in ['num_poliza']]))

        #agrupamos por poliza
        tarifas_agg = tarifas.groupBy("num_poliza").agg(fn.collect_list("datos_tarifa").alias("datos_tarifa"))
        return tarifas_agg

    def cuentas_cliente(self)-> DataFrame:
        """
        Cruzamos los clientes para recuperar sus cuentas 
        Devuelve un dataframe
        """

        #realizamos en primer lugar la agrupación de las cuentas

        cuentas_001 = self.cuentas.withColumn("datos_cuenta", fn.struct([col for col in self.cuentas.columns if col not in ["idcliente"]]))

        #agrupamos por cliente
        cuentas_agg = cuentas_001.groupBy("idcliente").agg(fn.collect_list("datos_cuenta").alias("datos_cuenta"))

        cruzan_001 = self.fc.cruce_tablas(df1=self.clientes, df2=cuentas_agg, clave_cruce=["idcliente"],
            campos_df1="*", campos_df2=["datos_cuenta"], tipo_cruce=7)
        
        return cruzan_001
    
    def datos_poliza(self, siniestros : DataFrame, recibos: DataFrame, tarifas : DataFrame)-> DataFrame:
        """
        Recibimos los siniestros, recibos y tarifas.

        Lo cruzamos uno a uno y obtenemos el dataframe con los datos de las pólizas 
        """
        clave_siniestro = [self.polizas.numpoliza == siniestros.idpoliza]

        cruzan_001 = self.fc.cruce_tablas(df1 = self.polizas, df2 = siniestros, clave_cruce=clave_siniestro, campos_df1="*",
            campos_df2=["datos_sin"],tipo_cruce=7)
        
        #realizamos el cruce con los recibos
        clave_recibos = [cruzan_001.numpoliza == recibos.idpoliza]
        cruzan_002 = self.fc.cruce_tablas(df1=cruzan_001, df2=recibos, clave_cruce=clave_recibos, campos_df1="*",
            campos_df2=["datos_recibos"], tipo_cruce=7)
        
        #cruzamos contra las tarifas
        clave_tarifas = [cruzan_002.numpoliza == tarifas.num_poliza]
        cruzan_003 = self.fc.cruce_tablas(df1=cruzan_002, df2=tarifas, clave_cruce=clave_tarifas, campos_df1="*", 
            campos_df2=["datos_tarifa"])

        return cruzan_003
    
    def intervinientes_tra(self, clientes : DataFrame)-> DataFrame:
        """
        Recuperamos los datos de los clientes y los agrupamos por poliza
        """
        cruzan_001 = self.fc.cruce_tablas(df1=self.intervinientes, df2 = clientes, clave_cruce=["idcliente"],campos_df1="*",
            campos_df2=[col for col in clientes.columns if col not in ["idcliente"]], tipo_cruce=7)

        #ahora que tenemos los datos de los clientes en sus pólizas agrupamos por poliza
        inter_001 = cruzan_001.withColumn("datos_cliente", fn.struct([col for col in cruzan_001.columns if col not in ["numpoliza"]]))

        #agrupamos por poliza
        inter_agg = inter_001.groupBy("numpoliza").agg(fn.collect_list("datos_cliente").alias("datos_cliente"))

        return inter_agg
    
    def fichero_final(self,polizas : DataFrame, intervinientes : DataFrame)->None:
        """
        Cruzamos el fichero de polizas y el de clientes y realizamos la carga como parquet que posteriormente irá a Big Query
        """

        fichero_final = self.fc.cruce_tablas(df1=polizas, df2=intervinientes, clave_cruce=["numpoliza"],
            campos_df1="*", campos_df2=["datos_cliente"])

        #realizamos la carga final
        fichero_final.coalesce(1).write.parquet("gs://uned_master_bucket_spark_nbu/parquet_tfm/fichero_final")

        

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    
    #generamos la clase
    etl = ingesta_datos(spark)
