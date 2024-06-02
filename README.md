# Curso ST0263 Tópicos Especiales en Telemática
# Informe: Map/Reduce en Python con MRJOB.

## Despligue EMR con AWS CLI

Para desplegar el cluster, al ya tener una instancia de s3 y llave privada creada lo instanciamos de la siguiente forma

```sh
aws emr create-cluster \
--release-label "emr-7.1.0" \
--name "lab-emr-drpalaciod" \
--applications Name=Spark Name=Hadoop Name=Pig Name=Hive \
--ec2-attributes KeyName=lab-06 \
--instance-type m5.xlarge \
--instance-count 3 \
--use-default-roles \
--no-auto-terminate \
--log-uri  "s3://lab-emr-bucket"
```
Donde:
```--release-label``` : Versión el cluster

```--name``` : Nombre de el cluster

```--applications``` : Aplicaciones a instalar en el cluster

```--ec2-attributes``` : Atributos de las instancias, determinamos el nombre de la llave que ultilizaremos para acceder a las instancias

```--instance-type``` : Tipo de instancia para las maquinas de el cluster.

```--instance-count``` : Numero de instancias de el cluster, una de ellas esta reservada para el main node, el resto serán de tipo cluster node

```--use-default-roles``` : Las instancias usaran sus roles de permisos default

```--no-auto-terminate``` : Normalmente EMR apaga los clusteres cuando no son ultizados por determinado tiempo, esta opción apagará los clusters solo cuando lo hagamos manualmente.


```--log-uri``` : El URI de el s3 para almacenar los logs

Luego de crear el cluster e instalar git con yum y mrjob con pip, clonamos este repositorio.

Con el comando hdfs dfs -copyFromLocal copiamos el dataset a el HDFS

Para correr MRJob en un cluster de hadoop, para efectos de este laboratorio, podremos usar esta estructura

```sh
python <mrjob>.py hdfs://<path absoluto dataset>  -r hadoop --output-dir hdfs://<path absoluto donde se guardará el output>
```

Por ejemplo al ejecutar el mrjob de word count de ejemplo usamos este comando:
```sh
python wordcount-mr.py hdfs:///user/hadoop/datasets/gutenberg-sma
ll/*  -r hadoop --output-dir hdfs:///user/hadoop/resultsexample.txt
```

La salida es un archivo en el HDFS guardado en la ruta definida en ```--output-dir``` que podemos extraer con el comando ```hdfs dfs -copyToLocal```

Este archivo realmente sera un directorio que esta distribuido en partes, evidenciando el manejo de archivos de el hdfs

![image](https://github.com/DanielPalacios05/Laboratorio-N6-MapReduce/assets/82727314/39bdffe0-b48c-4374-a6e4-7c18c5fd8276)

El contenido de cada parte corresponde a el ouput de el programa, es decir, el conteo de palabras

![image](https://github.com/DanielPalacios05/Laboratorio-N6-MapReduce/assets/82727314/b8d0a17d-c6c2-4732-b81e-eba7637d1ca0)


# Ejemplos de uso en Map/Reduce

1. Se tiene un conjunto de datos, que representan el salario anual de los empleados formales en Colombia por sector económico, según la DIAN. [datasets de ejemplo](../datasets/otros)

    *  La estructura del archivo es: (sececon: sector económico) (archivo: dataempleados.csv)

        idemp,sececon,salary,year

        3233,1234,35000,1960
        3233,5434,36000,1961
        1115,3432,34000,1980
        3233,1234,40000,1965
        1115,1212,77000,1980
        1115,1412,76000,1981
        1116,1412,76000,1982

    *  Realizar un programa en Map/Reduce, con hadoop en Python, que permita calcular:

        1. El salario promedio por Sector Económico (SE)
        2. El salario promedio por Empleado
        3. Número de SE por Empleado que ha tenido a lo largo de la estadística
     
   [Script de solucion](employee_jobs.py)

2. Se tiene un conjunto de acciones de la bolsa, en la cual se reporta a diario el valor promedio por acción, la estructura de los datos es (archivo: dataempresas.csv):

    company,price,date

    exito,77.5,2015-01-01
    EPM,23,2015-01-01
    exito,80,2015-01-02
    EPM,22,2015-01-02
    …

    * Realizar un programa en Map/Reduce, con hadoop en Python, que permita calcular:

        1. Por acción, dia-menor-valor, día-mayor-valor
        2. Listado de acciones que siempre han subido o se mantienen estables.
        3. DIA NEGRO: Saque el día en el que la mayor cantidad de acciones tienen el menor valor de acción (DESPLOME), suponga una inflación independiente del tiempo.
     
   [Script de solucion](stock_analysis.py)

3. Sistema de evaluación de películas (archivo: datapeliculas.csv): Se tiene un conjunto de datos en el cual se evalúan las películas con un rating, con la siguiente estructura:

    User,Movie,Rating,Genre,Date

    166,346,1,accion,2014-03-20
    298,474,4,accion,2014-03-20
    115,265,2,accion,2014-03-20
    253,465,5,accion,2014-03-20
    305,451,3,accion,2014-03-20
    …
    …

    * Realizar un programa en Map/Reduce, con hadoop en Python, que permita calcular:

        1. Número de películas vista por un usuario, valor promedio de calificación
        2. Día en que más películas se han visto
        3. Día en que menos películas se han visto
        4. Número de usuarios que ven una misma película y el rating promedio
        5. Día en que peor evaluación en promedio han dado los usuarios
        6. Día en que mejor evaluación han dado los usuarios
        7. La mejor y peor película evaluada por genero
           
   [Script de solucion](movie_analysis.py)




