from diagrams import Cluster, Diagram
from diagrams.gcp.analytics import BigQuery, Dataflow, PubSub, Dataproc
from diagrams.gcp.compute import AppEngine, Functions
from diagrams.gcp.database import BigTable
from diagrams.gcp.iot import IotCore
from diagrams.gcp.storage import GCS
from diagrams.custom import Custom
from diagrams.onprem.analytics import Spark


with Diagram("Proceso de Carga", show=False):
    #base_datos = Custom("Base de datos","./recursos/database.png")



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

    with Cluster("Dataproc"):
        with Cluster("ETL"):
            step003 = Spark("")
            step004 = step003 >> Custom("","./recursos/parquet_nofondo.png") 
    
    step005 = step004 >> GCS("Google Cloud Storage")

    with Cluster("Entorno analítico"):
        step006 = step005 >> BigQuery("Big Query")
