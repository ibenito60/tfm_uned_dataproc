from diagrams import Cluster, Diagram
from diagrams.gcp.analytics import BigQuery, Dataflow, PubSub, Dataproc
from diagrams.gcp.compute import AppEngine, Functions
from diagrams.gcp.database import BigTable
from diagrams.gcp.iot import IotCore
from diagrams.gcp.storage import GCS
from diagrams.custom import Custom
from diagrams.onprem.analytics import Spark


with Diagram("Solución técnica bbdd", show=False):
    #base_datos = Custom("Base de datos","./recursos/database.png")

    almacen = GCS("Almacenamiento ficheros")

    """
    with Cluster("Origen de datos"):
        step001 = [
    Custom("polizas","./recursos/txt_file.png"),
    Custom("siniestros","./recursos/txt_file.png"),
    Custom("pagos","./recursos/txt_file.png"),
    Custom("recibos","./recursos/txt_file.png"),
    Custom("clientes","./recursos/txt_file.png"),
    Custom("tarifas","./recursos/txt_file.png"),
    Custom("cuentas","./recursos/txt_file.png"),
    Custom("intervinientes","./recursos/txt_file.png")
        ] >> almacen
        #flow = base_datos >> almacen
    """
    with Cluster("Ficheros"):
        step001 = Custom("Base de datos","./recursos/database.png") >> almacen
    with Cluster("Dataproc"):
        step002 = step001 >> Dataproc("Dataproc")
        with Cluster("ETL"):
            step003 = step002 >> Spark("")
            step004 = step003 >> Custom("","./recursos/parquet_nofondo.png") 
    
    step005 = step004 >> GCS("Almacenamiento Parquet")

    with Cluster("Entorno analítico"):
        step006 = step005 >> BigQuery("Big Query")
        with Cluster("Cuadro de mando"):
            step006 >> Custom("Data Studio","./recursos/data_studio.png")


"""
with Diagram("Message Collecting", show=False):
    pubsub = PubSub("pubsub")

    with Cluster("Source of Data"):
        [IotCore("core1"),
         IotCore("core2"),
         IotCore("core3")] >> pubsub

    with Cluster("Targets"):
        with Cluster("Data Flow"):
            flow = Dataflow("data flow")

        with Cluster("Data Lake"):
            flow >> [BigQuery("bq"),
                     GCS("storage")]

        with Cluster("Event Driven"):
            with Cluster("Processing"):
                flow >> AppEngine("engine") >> BigTable("bigtable")

            with Cluster("Serverless"):
                flow >> Functions("func") >> AppEngine("appengine")

    pubsub >> flow
"""
