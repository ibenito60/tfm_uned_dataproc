#!/bin/bash

#definimos las variables que vamos a usar en el proyecto
export REGION=europe-west1-c
export PROJECT_ID=feisty-nectar-353011
export CLUSTER_REGION=europe-west1
export CLUSTER=cluster-uned-nbu
export ALMACEN=uned_master_bucket_spark_nbu
export NUMWORKERS=2
export TAM_DISCO=500
export TIEMPO_MAX=14400s


function helpPanel(){
    echo -e "Proceso de producción. Constamos con un argumento de entrada."
    echo -e "-proceso: Valores definidos:"
    echo -e "cluster. Creacción del cluster"
    echo -e "job_spark. Lanzar el proceso de Spark"
    echo -e "load_bq. Lanzar la carga en Big Query"
    echo -e "total. Lanzar todo el proceso"
    echo -e "total_no_cl. Lanzar el proceso sin la ejecución del cluster"
    echo -e "borrar_cluster. Borrado del cluster"
    exit 0
}


function create_cluster(){
    gcloud beta dataproc clusters create $CLUSTER \
    --enable-component-gateway \
    --bucket $ALMACEN \
    --region $CLUSTER_REGION \
    --zone $REGION \
    --master-machine-type n1-standard-2 \
    --master-boot-disk-size $TAM_DISCO \
    --num-workers $NUMWORKERS \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size $TAM_DISCO \
    --image-version 1.4-debian10 \
    --optional-components ANACONDA,JUPYTER,ZOOKEEPER \
    --max-age $TIEMPO_MAX \
    --initialization-actions 'gs://goog-dataproc-initialization-actions-europe-west1/hue/hue.sh' \
    --project $PROJECT_ID
    #recogemos el código que devuelve
    ret_cluster=$?
    return $ret_cluster
}

function spark_job(){
    #lanzamos el script del proceso Pyspark
    gcloud dataproc jobs submit pyspark /home/$USER/scripts_tfm/etl_001.py > /home/$USER/app.log 2>&1 \
    --cluster=$CLUSTER \
    --region=$CLUSTER_REGION  \
    #recuperamos y devolvemos el código de salida
    ret_spark=$?
    return $ret_spark
}

function load_bq(){
    bq load \
    --source_format=PARQUET \
    --parquet_enable_list_inference=true \
    --replace=true \
    prueba_json_bq.fichero_total \
    "gs://uned_master_bucket_spark_nbu/parquet_tfm/fichero_final/*.parquet"
    #cogemos el cógido de retorno de la carga en BQ
    ret_bq=$?
    return $ret_bq
}


function borrar_cluster(){
    #pendiente de borrar el cluster
    echo -e "Borrando el cluster..."
    gcloud dataproc clusters delete $CLUSTER \
    --region=$CLUSTER_REGION
    #recuperamos el código de salida
    ret_borrar=$?
    return $ret_borrar
}


function ejecucion(){

    if [ "$(echo $arg_proceso)" == "cluster" ]; then
        create_cluster
    elif [ "$(echo $arg_proceso)" == "job_spark" ]; then
        spark_job
    elif [ "$(echo $arg_proceso)" == "load_bq" ]; then
        load_bq
    elif [ "$(echo $arg_proceso)" == "total" ]; then
        #ejecutamos la creaccion del cluster
        create_cluster
        #recuperamos el codigo que devuelve
        ret_create_cluster=$?
        #condiciones para seguir avanzando
        if [ ${ret_create_cluster} != 0 ]; then
            echo -e "Problemas al generar el cluster fin del proceso con errores."
            exit $ret_create_cluster
        else
            #seguimos con el proceso
            echo -e "Lanzamos el job de Spark"
            #lanzamos el job de spark
            spark_job
            #recuperamos el valor que devuelve
            ret_spark_job=$?
            if [ ${ret_spark_job} != 0 ]; then
                echo -e "Problemas al ejecutar el job de Spark."
                echo -e "Fin de la ejecución con errores"
                exit $ret_spark_job
            else
                echo -e "Ejecutado correcto el job de Spark. Seguimos con el proceso"
                echo -e "Lanzamos la carga en Big Query"
                load_bq
                #recuperamos el valor que devuelve
                ret_load_bq=$?
                if [ ${ret_load_bq} != 0 ]; then 
                    echo -e "La carga en Big Query no se ha realizado de manera correcta."
                    echo -e "Fin de la ejecución con errores"
                    exit $ret_load_bq
                else
                    echo -e "Fin del proceso correctamente."
                    exit 0
                fi

            fi

        fi

    elif [ "$(echo $arg_proceso)" == "total_no_cl" ]; then
        #seguimos con el proceso
        echo -e "Lanzamos el job de Spark"
        #lanzamos el job de spark
        spark_job
        #recuperamos el valor que devuelve
        ret_spark_job=$?
        if [ ${ret_spark_job} != 0 ]; then
            echo -e "Problemas al ejecutar el job de Spark."
            echo -e "Fin de la ejecución con errores"
            exit $ret_spark_job
        else
            echo -e "Ejecutado correcto el job de Spark. Seguimos con el proceso"
            echo -e "Lanzamos la carga en Big Query"
            load_bq
            #recuperamos el valor que devuelve
            ret_load_bq=$?
            if [ ${ret_load_bq} != 0 ]; then 
                echo -e "La carga en Big Query no se ha realizado de manera correcta."
                echo -e "Fin de la ejecución con errores"
                exit $ret_load_bq
            else
                echo -e "Fin del proceso correctamente."
                exit 0
            fi
        fi

    elif [ "$(echo $arg_proceso)" == "borrar_cluster" ]; then
        borrar_cluster
        ret_borrado=$?
        if [ ${ret_borrado} != 0 ]; then
            echo -e "No se ha podido borrar el cluster"
            exit 1
        else
            echo -e "Borrado del cluster correcto."
            exit 0
        fi
    else
        echo -e "Este modo de proceso no es valido"
    fi
}


declare -i contador=0; while getopts ":p:h:" arg; do
	case $arg in
		p) arg_proceso=$OPTARG; let contador+=1 ;;
		h) helpPanel;;
	esac
done

#funcion principal
if [ $contador -ne 1 ]; then
	helpPanel
else
    ejecucion
fi
