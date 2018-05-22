# bigdata

Repository that contains all the projects developed for the BigData course of the Politecnico di Torino

## create new project in intellij

create a new project from source, import the libraries trough the "project structure" menu: the libraries are in the `labs/lib` folder 


## How to submit a MapReduce job to the bigdata@polito Hadoop cluster

* Compile the MapReduce Application obtaining the jar file.

In Intellij IDEA, go to File->Project Strcture-> Artifacts -> generate artifacts, select as main class the Driver.

* scp the jar file to the gateway using: `scp <idea-project-folder>/out/artifacts/main_jar/main.jar s241915@bigdatalab.polito.it:/home/bigdata-01QYD/s241915/`

* use HDFS commands to copy the input files into the cluster file system, or connect to: https://bigdatalab.polito.it:8080 to use the web interface (Hue).

via web interface: 

-- drawer -> Files to go into the HDFS user home directory

-- upload the input file


* execute the jar from the gateway: `hadoop jar app.jar <number-of-reducers> <input-file> <outputfile>`

all the relative path in input and output file are related to the home directory of the user in the hadoop cluster.

The whole job history is available at https://ma1-bigdata.polito.it:19890/jobhistory/, kerberos authentication required: obtain a 1 day ticket for the local machine using `kinit s241915`


## How to submit a Spark job locally or on the cluster

Differently from MapReduce, a spark job can be executed locally or on the hadoop cluster using the YARN scheduler.

To submit a spark application, the `spark-submit` CLI must be used. It accepts various parameters, values separated using space and not equal


* `--class` class of the jar to be executed that contains the main
* `--master` used to specify the scheduler. Values: `yarn` to execture the application on the cluster, `loca` to execute on the local workstation.
* `--deploy-mode` used to specify where the driver must be executed. values: `client` or `cluster`. Note that all the local variable of the driver will be istantiated in the driver, in cluster mode the maximum amout of main memry for each executor is 2 to 8GB, better to use client mode if the workstation has more than 8GB of ram.
* `--num-executors`, default is 2
* `--executor-cores`, default is 1
* `--executor-memory`, default is 1GB, main memory for each executor that runs on the cluster
* `--driver-cores`
* `--driver-memory`

example: 

```spark-submit --class it.polito.bigdata.spark.DriverMyApp --deploy-mode cluster --master yarn MyApp.jar <arguments>```


## How to Configure Kerberos authentication

A Kerberos ticket is required to have access to other servers of the bigdatalab other that the gateway.

* `sudo apt install krb5-user` and then follow the [guide](https://bigdata.polito.it/content/access-instructions) 