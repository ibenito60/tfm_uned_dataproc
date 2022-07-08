import pyspark
from datacleansing.funciones import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


#realizamos la lectura de los 8 ficheros txt 

polizas = spark.read.csv("",inferSchema=True, sep=',', header=True)
siniestros = spark.read.csv("",inferSchema=True, sep=',', header=True)
pagos = spark.read.csv("",inferSchema=True, sep=',', header=True)
recibos = spark.read.csv("",inferSchema=True, sep=',', header=True)
clientes = spark.read.csv("",inferSchema=True, sep=',', header=True)
intervinientes = spark.read.csv("",inferSchema=True, sep=',', header=True)
cuentas = spark.read.csv("",inferSchema=True, sep=',', header=True)
tarifas = spark.read.csv("",inferSchema=True, sep=',', header=True)

fc = clase_func_comunes()

#primera parte de polizas y pagos 
clave_pol = [polizas.numpoliza == siniestros.idpoliza]
cruzan_sini = fc.cruce_tablas(df1=siniestros, df2=polizas, clave_cruce=clave_pol, 
campos_df1="*", campos_df2=['estado','fecha_baja','causa_baja','fecha_baja'], tipo_cruce=1)

transformado_sini = cruzan_sini.withColumn('fecha_apertura', fn.when((fn.col("pol_estado") == 'PA'),fn.col("fecha_baja").otherwise(fn.col("fecha_apertura"))))\
    .withColumn("tipo_siniestro")

