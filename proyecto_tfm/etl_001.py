from distutils import log
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as fn
from pyspark.sql.types import *
from datetime import datetime, timedelta
import getpass
from pyspark import StorageLevel
import logging
import google.cloud.logging


class clase_func_comunes:

    fecha_activa = "9999-12-31"
    fecha_activa_2 = "0001-01-01"
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")
    fecha_anio = datetime.now().year
    fecha_ayer = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # Guardar en memoria dataframes
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
        """
        Realizamos la llamada a los método que cargan los ficheros y les dan esquema.
        Se encapsula, ya que en caso de que no se encuentre el fichero y no pueda cargar terminamos con la aplicación
        """
        self.fc = clase_func_comunes()
        self.hora_inicio = datetime.now()
        logging.info(f"Inicio del proceso. Hora de inicio: {self.hora_inicio}")
        self.spark = spark
        self.sc = self.spark.sparkContext
        
        try:
            self.polizas = self.get_tabla_polizas()
            self.siniestros = self.get_tabla_siniestros()
            self.pagos = self.get_tabla_pagos()
            self.recibos = self.get_tabla_recibos()
            self.clientes = self.get_tabla_clientes()
            self.intervinientes = self.get_tabla_intervinientes()
            self.cuentas = self.get_tabla_cuentas()
            self.tarifas = self.get_tabla_tarifas()
        except Exception as e:
            logging.error(f"Fallado la lectura de los ficheros debido a {e}.")
            hora_fin = datetime.now()
            logging.info(f"Fin de la ejecución. Tiempo de ejecución: {hora_fin - self.hora_inicio}")
            sys.exit(31)
            
    
    def get_tabla_cuentas(self)-> DataFrame:
        """
        Cargamos el fichero de las cuentas personales de los clientes
        :return tabla_cuentas
        """
        #definimos el esquema de las pólizas
        schema_cuentas = StructType([ \
            StructField("idcuenta",IntegerType(),True), \
            StructField("idcliente",IntegerType(),True),\
            StructField("empresa",StringType(),True), \
            StructField("oficina",StringType(),True), \
            StructField("digcontrol", StringType(), True), \
            StructField("contrato", StringType(), True), \
            StructField("situacion", StringType(), True), \
            StructField("fecha_alta",StringType(),True),\
            StructField("fecha_baja",StringType(),True),\
            StructField("iban_total",StringType(),True)
        ])

        #realizamos la lectura de la tabla
        tabla_cuentas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/cuentas.txt",schema=schema_cuentas, sep=',', header=True)
        return tabla_cuentas
    
    def get_tabla_clientes(self)-> DataFrame:
        """
        Cargamos el fichero de clientes
        :return tabla_clientes
        """
        #definimos el esquema de las pólizas
        schema_clientes = StructType([ \
            StructField("idcliente",IntegerType(),False), \
            StructField("nombre",StringType(),True), \
            StructField("apellidos",StringType(),True), \
            StructField("fecnaci", StringType(), True), \
            StructField("direccion", StringType(), True), \
            StructField("numdni", StringType(), True)
        ])

        #realizamos la lectura de la tabla
        tabla_clientes = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/clientes.txt",schema=schema_clientes, sep=',', header=True)
        return tabla_clientes
    
    def get_tabla_siniestros(self)-> DataFrame:
        """
        Cargamos el fichero de siniestros.
        :returns tabla_siniestros
        """
        #definimos el esquema de las pólizas
        schema_siniestros = StructType([ \
            StructField("idsiniestro",IntegerType(),False), \
            StructField("idpoliza",IntegerType(),False), \
            StructField("fecha_ocurs",StringType(),True), \
            StructField("fecha_apertura", StringType(), True), \
            StructField("fecha_cierre", StringType(), True), \
            StructField("tipo_siniestro", StringType(), True), \
            StructField("estado",StringType(),True)
        ])

        #realizamos la lectura de la tabla
        tabla_siniestros = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/siniestros.txt",schema=schema_siniestros, sep=',', header=True)
        return tabla_siniestros

    def get_tabla_pagos(self)-> DataFrame:
        """
        Cargamos el fichero de pagos de los siniestros
        :returns tabla_pagos
        """
        #definimos el esquema de las pólizas
        schema_pagos = StructType([ \
            StructField("idpago",IntegerType(),False), \
            StructField("idsiniestro",IntegerType(),False), \
            StructField("estado",StringType(),True), \
            StructField("fecha_pago", StringType(), True), \
            StructField("importe_pago", DoubleType(), True), \
            StructField("tipo_pago", StringType(), True)
        ])

        #realizamos la lectura de la tabla
        tabla_pagos = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/pagos_siniestro.txt",schema=schema_pagos, sep=',', header=True)
        return tabla_pagos
    
    def get_tabla_tarifas(self)-> DataFrame:
        """
        Cargamos el fichero de las tarifas de las pólizas
        :returns tabla_tarifas
        """
        #definimos el esquema de las pólizas
        schema_tarifas = StructType([ \
            StructField("num_poliza",IntegerType(),False), \
            StructField("tarifa",StringType(),False), \
            StructField("situacion",StringType(),True), \
            StructField("fecha_alta", StringType(), True), \
            StructField("fecha_baja", StringType(), True), \
            StructField("importe_prima", StringType(), True)
        ])

        #realizamos la lectura de la tabla
        tabla_tarifas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/tarifas.txt",schema=schema_tarifas, sep=',', header=True)
        return tabla_tarifas

    def get_tabla_recibos(self)-> DataFrame:
        """
        Cargamos el fichero de los recibos de las pólizas
        :returns tabla_recibos
        """
        #definimos el esquema de las pólizas
        schema_recibos = StructType([ \
            StructField("idrecibo",IntegerType(),False), \
            StructField("fecha_emision",StringType(),True), \
            StructField("fecha_efecto",StringType(),True), \
            StructField("idpoliza", IntegerType(), False), \
            StructField("situacion", StringType(), True), \
            StructField("origen", StringType(), True), \
            StructField("importe_prima",DoubleType(),True),\
            StructField("importe_consorcio",DoubleType(),True),\
            StructField("importe_comision",DoubleType(),True)
        ])

        #realizamos la lectura de la tabla
        tabla_recibos = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/recibos.txt",schema=schema_recibos, sep=',', header=True)
        return tabla_recibos

    def get_tabla_intervinientes(self)-> DataFrame:
        """
        Cargamos el fichero de intervinientes
        :returns tabla_intervinientes
        """
        #definimos el esquema de las pólizas
        schema_intervinientes = StructType([ \
            StructField("idcliente",IntegerType(),False), \
            StructField("numpoliza",IntegerType(),True), \
            StructField("tipo_intervencio",StringType(),True), \
            StructField("estado", StringType(), True), \
            StructField("fecha_alta", StringType(), True), \
            StructField("fecha_baja", StringType(), True), \
            StructField("orden_inter",StringType(),True)
        ])

        #realizamos la lectura de la tabla
        tabla_intervinientes = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/intervinientes.txt",schema=schema_intervinientes, sep=',', header=True)
        return tabla_intervinientes   
    
    def get_tabla_polizas(self)-> DataFrame:
        """
        Cargamos las pólizas del fichero y le proporcionamos el esquema correcto
        :returns tabla_polizas
        """
        #definimos el esquema de las pólizas
        schema_poliza = StructType([ \
            StructField("numpoliza",IntegerType(),False), \
            StructField("pol_estado",StringType(),True), \
            StructField("fecha_efecto",StringType(),True), \
            StructField("fecha_sol", StringType(), True), \
            StructField("fecha_vencimiento", StringType(), True), \
            StructField("formapago", StringType(), True), \
            StructField("fecha_baja",StringType(),True), \
            StructField("causa_baja", StringType(), True), \
            StructField("fecha_suspension", StringType(), True), \
            StructField("causa_suspen", StringType(), True)
        ])

        #realizamos la lectura de la tabla
        tabla_polizas = self.spark.read.csv("gs://uned_master_bucket_spark_nbu/csv_tfm/polizas.txt",schema=schema_poliza, sep=',', header=True)
        return tabla_polizas
    
       
    def siniestros_pagos(self)-> DataFrame:
        """
        Construimos los siniestros y los pagos de los siniestros

        Renombramos la columna de estado de los pagos para evitar posibles duplicidades de nombres en un futuro.
        Creamos un columna de tipo struct con todos los campos de la tabla de pagos excepto con la clave del siniestro. Ya que es información que ya tenemos en la tabla
        de siniestros.

        Realizamos el mismo paso con los siniestros pero quitando el campo de la póliza.

        Agrupamos y devolvemos el dataframe.

        :returns siniestros_agg
        """
        logging.info("Construcción de la estructura relativa a siniestros")
        pagos_siniestros = self.pagos.withColumnRenamed('estado_pago','estado')
        siniestros = self.siniestros
        #agrupamos los siniestros
        pagos_siniestros_struct = pagos_siniestros.withColumn("datos_pago", fn.struct([col for col in pagos_siniestros.columns if col not in ["idsiniestro"]]))
        pagos_agg = pagos_siniestros_struct.groupBy("idsiniestro").agg(fn.collect_list("datos_pago").alias("datos_pago"))

        #realizamos el cruce entre los pagos y los siniestros por siniestro
        cruzan_001 = self.fc.cruce_tablas(df1=siniestros, df2=pagos_agg, clave_cruce=['idsiniestro'], campos_df1="*",
            campos_df2=["datos_pago"], tipo_cruce=7)
        
        #realizo la agrupacion de los siniestros
        siniestros_struct = cruzan_001.withColumn("datos_sin", fn.struct([col for col in cruzan_001.columns if col not in ["idpoliza"]]))
        siniestros_agg = siniestros_struct.groupBy("idpoliza").agg(fn.collect_list("datos_sin").alias("datos_sin"))
        
        logging.info("Fin de la construcción de la estructura de siniestros")
        return siniestros_agg
    
    def recibos_tra(self)-> DataFrame:
        """
        Construimos los recibos.
        Agrupamos en la columna de tipo struct datos_recibos todos los campos de la tabla recibo excepto el número de póliza.

        Agrupamos y devolvemos el dataframe.

        :returns recibos_agg
        """
        logging.info("Iniciamos la construcción de la estructura relativa a recibos")
        recibos = self.recibos.withColumn("datos_recibos", fn.struct([col for col in self.recibos.columns if col not in ['idpoliza']]))
        #agrupamos por poliza
        recibos_agg = recibos.groupBy("idpoliza").agg(fn.collect_list("datos_recibos").alias("datos_recibos"))
        logging.info("Fin de la construcción de la estructura de recibos")
        return recibos_agg

    def tarifas_tra(self)-> DataFrame:
        """
        Construimos las tarifas.
        Agrupamos en la columna de tipo struct datos_tarifa todos los campos de la tabla taridas excepto el número de póliza.

        Agrupamos y devolvemos el dataframe.

        :returns tarifas_agg
        """
        logging.info("Iniciamos la construcción de la estructura relativa a las tarifas")
        tarifas = self.tarifas.withColumn("datos_tarifa", fn.struct([col for col in self.tarifas.columns if col not in ['num_poliza']]))

        #agrupamos por poliza
        tarifas_agg = tarifas.groupBy("num_poliza").agg(fn.collect_list("datos_tarifa").alias("datos_tarifa"))
        logging.info("Fin de la construcción de la estructura de tarifas")
        return tarifas_agg

    def cuentas_cliente(self)-> DataFrame:
        """
        Construimos las cuentas de los clientes.
        Agrupamos, cruzamos y devolvemos el dataframe.

        :returns cruzan_001
        """

        #realizamos en primer lugar la agrupación de las cuentas
        logging.info("Iniciamos la construcción de los datos de la cuenta de los clientes")
        cuentas_001 = self.cuentas.withColumn("datos_cuenta", fn.struct([col for col in self.cuentas.columns if col not in ["idcliente"]]))

        #agrupamos por cliente
        cuentas_agg = cuentas_001.groupBy("idcliente").agg(fn.collect_list("datos_cuenta").alias("datos_cuenta"))

        cruzan_001 = self.fc.cruce_tablas(df1=self.clientes, df2=cuentas_agg, clave_cruce=["idcliente"],
            campos_df1="*", campos_df2=["datos_cuenta"], tipo_cruce=7)
        logging.info("Fin de la construcción de las cuentas de los clientes")
        return cruzan_001
    
    def datos_poliza(self, siniestros : DataFrame, recibos: DataFrame, tarifas : DataFrame)-> DataFrame:
        """
        :param siniestros
        :param recibos
        :param tarifas

        Recibimos de entrada los 3 dataframes que contienen la información relativa a los siniestros, recibos y tarifas.

        Los vamos cruzando de uno en uno de tipo left para recuperar todos los datos.

        :returns cruzan_003 
        """
        logging.info("Inicio de la agrupación de los datos relativos a las pólizas")
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
        logging.info("Fin de la agrupación de los datos relativos a las pólizas")
        return cruzan_003
    
    def intervinientes_tra(self, clientes : DataFrame)-> DataFrame:
        """
        :param clientes
        Recuperamos los datos de los clientes y los agrupamos por poliza
        :returns inter_agg
        """
        logging.info("Inicio de la construcción de los datos de los clientes según tipo de intervención")
        cruzan_001 = self.fc.cruce_tablas(df1=self.intervinientes, df2 = clientes, clave_cruce=["idcliente"],campos_df1="*",
            campos_df2=[col for col in clientes.columns if col not in ["idcliente"]], tipo_cruce=7)

        #ahora que tenemos los datos de los clientes en sus pólizas agrupamos por poliza
        inter_001 = cruzan_001.withColumn("datos_cliente", fn.struct([col for col in cruzan_001.columns if col not in ["numpoliza"]]))

        #agrupamos por poliza
        inter_agg = inter_001.groupBy("numpoliza").agg(fn.collect_list("datos_cliente").alias("datos_cliente"))
        logging.info("Fin de la agrupación de los datos de los clientes")
        return inter_agg
    
    def fichero_final(self,polizas : DataFrame, intervinientes : DataFrame)->None:
        """
        :param polizas
        :param intervinientes

        Cruzamos el fichero de polizas y el de clientes y realizamos la carga como parquet que posteriormente irá a Big Query
        """

        fichero_final = self.fc.cruce_tablas(df1=polizas, df2=intervinientes, clave_cruce=["numpoliza"],
            campos_df1="*", campos_df2=["datos_cliente"])

        #realizamos la carga final
        logging.critical("Inicio de la carga del fichero final")
        try:
            inicio_carga = datetime.now()
            self.sc.setJobGroup("Proceso de carga", "Carga final del proceso en Parquet")
            fichero_final.coalesce(1).write.parquet("gs://uned_master_bucket_spark_nbu/parquet_tfm/fichero_final",mode='overwrite')
            fin_carga = datetime.now()
            logging.info(f"Fin de la carga OK. Tiempo de ejecución: {fin_carga - inicio_carga}")
            logging.info(f"Fin de la ejecución. Tiempo de ejecución: {fin_carga - self.hora_inicio}")
        except Exception as e:
            logging.error(f"No se ha podido realizar la carga del fichero debido a {e}")
            hora_fin = datetime.now()
            logging.info(f"Fin de la ejecución. Tiempo de ejecución: {hora_fin - self.hora_inicio}")
            sys.exit(32)
        

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(filename='/home/thefirmatm/app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',level = logging.INFO)
    #generamos la clase
    etl = ingesta_datos(spark)
    siniestros = etl.siniestros_pagos()
    recibos = etl.recibos_tra()
    tarifas = etl.tarifas_tra()
    cuentas_clientes = etl.cuentas_cliente()
    polizas = etl.datos_poliza(siniestros, recibos, tarifas)
    intervinientes = etl.intervinientes_tra(cuentas_clientes)
    etl.fichero_final(polizas,intervinientes)
