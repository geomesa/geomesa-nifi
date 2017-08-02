# GeoMesa NiFi

GeoMesa-NiFi allows you to ingest data into GeoMesa using the NiFi dataflow framework. Currently, the following datastores are supported:

* Accumulo
* HBase
* Kafka 09
* Kafka 10

# Building from Source

To build, simply clone and build with maven:

```bash
git clone git@github.com:geomesa/geomesa-nifi.git
cd geomesa-nifi
mvn clean install
```

# Installation

To install the GeoMesa processors you will need to copy the appropriate nar file into the ``lib`` directory of your NiFi installation. There are
three nar files that are built currently:

Nar                                | Datastores
---                                | ---
geomesa-nifi-nar-$VERSION.nar      | Accumulo, HBase
geomesa-nifi-kafka-09-$VERSION.nar | Kafka 09
geomesa-nifi-kafka-10-$VERSION.nar | Kafka 10

For example, to install the Acccumulo or HBase nar after building from
source:

```bash
cp geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-$VERSION.nar $NIFI_HOME/lib/
````

# SFTs and Converters

GeoMesa NiFi nar files package a set of predefined SimpleFeatureType schema definitions and GeoMesa Converter definitions for popular data
sources including:

* Twitter
* GDelt
* OpenStreetMaps

The full list of converters can be found in the GeoMesa source:

https://github.com/locationtech/geomesa/tree/master/geomesa-tools/conf/sfts

## Defining custom SFTs and Converters

There are two ways of providing custom SFTs and converters:

* Providing a ``reference.conf`` file via a jar in the ``lib`` dir
* Providing the config definitions via the Processor configuration

### SFTs and Converters on the Classpath

To bundle configuration in a jar file simply place your config in a file named ``reference.conf`` and place it in a jar file:

```bash
jar cvf data-formats.jar reference.conf
```

You can verify your jar was building properly:

```bash
$ jar tvf data-formats.jar
     0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
    69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
 28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf
```

### Definining SFTs and Converters via the UI

The preferred way of providing SFTs and Converters is direction in the Processor configuration via the NiFi UI. Simply copy/paste your typesafe
configuration into the ``SftSpec`` and ``ConverterSpec`` property 
fields.

# Processors

This project provides the following processors:

* ``PutGeoMesaAccumulo`` - Ingest data into GeoMesa Accumulo 
* ``PutGeoMesaHBase`` - Ingest data into GeoMesa HBase
* ``PutGeoMesaKafka_09`` - Ingest data into GeoMesa Kafka 0.9.x
* ``PutGeoMesaKafka_10`` - Ingest data into GeoMesa Kafka 0.10.x
* ``PutGeoTools`` - Ingest data into an arbitrary GeoTools Datastore based on parameters using a GeoMesa converter or avro
* ``ConvertToGeoAvro`` - Use a GeoMesa converter to create geoavro

## PutGeoMesaAccumulo

The ``PutGeoMesaAccumulo`` processor is used for ingesting data into an Accumulo backed GeoMesa datastore. To use this processor
first add it to the workspace and open the properties tab of its configuration. Descriptions of the properties are
given below:

Property                      | Description
---                           | ---
Mode                          | Converter or Avro file ingest mode switch.
SftName                       | Name of the SFT on the classpath to use. This property overrides SftSpec.
ConverterName                 | Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride           | Override the feature name on ingest.
SftSpec                       | SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec                 | Converter specification string. Overwritten by ConverterName if ConverterName is valid.
instanceId                    | Accumulo instance ID
zookeepers                    | Comma separated list of zookeeper IPs or hostnames
user                          | Accumulo username with create-table and write permissions
password                      | Accumulo password for given username
visibilities                  | Accumulo scan visibilities
tableName                     | Name of the table to write to. If using namespaces be sure to include that in the name.
writeThreads                  | Number of threads to use when writing data to GeoMesa, has a linear effect on CPU and memory usage
generateStats                 | Enable stats table generation
useMock                       | Use a mock instance of Accumulo
GeoMesa Configuration Service | Configuration service to use. More about this feature below.

### GeoMesa Configuration Service

The ``PutGeoMesa`` plugin supports [NiFi Controller Services](http://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)
to manage common configurations. This allows the user to specify a single location to store the Accumulo connection parameters.
This allows you to add new PutGeoMesa processors without having to enter duplicate data.

To add the ``GeomesaConfigControllerService`` access the ``Contoller Settings`` from NiFi global menu and navigate to the
``ControllerServices`` tab and click the ``+`` to add a new service. Search for the ``GeomesaConfigControllerService``
and click add. Edit the new service and enter the appropriate values for the properties listed.

To use this feature, after configuring the service, select the appropriate Geomesa Config Controller Service from the drop down
of the ``GeoMesa Configuration Service`` property. When a controller service is selected the ``zookeepers``, ``instanceId``,
``user``, ``password`` and ``tableName`` parameters are not required or used.

## PutGeoMesaHBase

The ``PutGeoMesaHBase`` processor is used for ingesting data into an HBase backed GeoMesa datastore. To use this processor
first add it to the workspace and open the properties tab of its configuration. Descriptions of the properties are
given below:

Property             | Description
---                  | ---
Mode                 | Converter or Avro file ingest mode switch.
SftName              | Name of the SFT on the classpath to use. This property overrides SftSpec.
ConverterName        | Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride  | Override the feature name on ingest.
SftSpec              | SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec        | Converter specification string. Overwritten by ConverterName if ConverterName is valid.
bigtable.table.name  | The base Catalog table name for GeoMesa in HBase
coprocessor.url      | A path to a jar file (likely the HBase distributed runtime, likely in HDFS) containing the GeoMesa HBase coprocessors
security.enabled     | Enable HBase Security (Visibilities) 


## PutGeoMesaKafka

The ``PutGeoMesaKafka`` processor is used for ingesting data into a Kafka backed GeoMesa datastore. This processor only supports Kafka 0.9
and newer. To use this processor first add it to the workspace and open the properties tab of its configuration. Descriptions of the 
properties are given below:

Property                    | Description
---                         | ---
Mode                        | Converter or Avro file ingest mode switch.
SftName                     | Name of the SFT on the classpath to use. This property overrides SftSpec.
ConverterName               | Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride         | Override the feature name on ingest.
SftSpec                     | SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec               | Converter specification string. Overwritten by ConverterName if ConverterName is valid.
brokers                     | List of Kafka brokers
zookeepers                  | Comma separated list of zookeeper IPs or hostnames
zkpath                      | Zookeeper path to Kafka instance
namespace                   | Kafka namespace to use
partitions                  | Number of partitions to use in Kafka topics
replication                 | Replication factor to use in Kafka topics
isProducer                  | Flag to mark if this is a producer
expirationPeriod            | Feature will be auto-dropped (expired) after this delay in milliseconds. Leave blank or use -1 to not drop features.
cleanUpCache                | Run a thread to clean up the live feature cache if set to true. False by default. Use 'cleanUpCachePeriod' to configure the length of time between cache cleanups. Every second by default.

## PutGeoTools

The ``PutGeoTools`` processor is used for ingesting data into a GeoTools compatible datastore. To use this processor
first add it to the workspace and open the properties tab of its configuration. Descriptions of the properties are
given below:

Property                    | Description
---                         | ---
Mode                        | Converter or Avro file ingest mode switch.
SftName                     | Name of the SFT on the classpath to use. This property overrides SftSpec.
ConverterName               | Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride         | Override the feature name on ingest.
SftSpec                     | SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec               | Converter specification string. Overwritten by ConverterName if ConverterName is valid.
DataStoreName               | Name of the datastore to ingest data into.

This processor also accepts dynamic parameters that may be needed for the specific datastore that you're trying to access.

## ConvertToGeoAvro

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal converter framework to convert features into Avro and pass them
along as a flow to be used by other processors in NiFi. To use this processor first add it to the workspace and open
the properties tab of its configuration. Descriptions of the properties are given below:

Property                    | Description
---                         | ---
Mode                        | Converter or Avro file ingest mode switch.
SftName                     | Name of the SFT on the classpath to use. This property override SftSpec.
ConverterName               | Name of converter on the classpath to use. This property overrides ConverterSpec.
FeatureNameOverride         | Override the feature name on ingest.
SftSpec                     | SFT specification String. Overwritten by SftName if SftName is valid.
ConverterSpec               | Converter specification string. Overwritten by ConverterName if ConverterName is valid.
OutputFormat                | Only Avro is supported at this time.

# NiFi User Notes

NiFi allows you to ingest data into GeoMesa from every source GeoMesa supports and more. Some of these sources can be tricky to setup and configure. 
Here we detail some of the problems we've encountered and how to resolve them. 

## GetHDFS Processor with Azure Integration

It is possible to use the [Hadoop Azure Support](https://hadoop.apache.org/docs/current/hadoop-azure/index.html) to access Azure Blob Storage using HDFS.
You can leverage this capability to have the GetHDFS processor pull data directly from the Azure Blob store. However, due to how the GetHDFS processor was 
written, the ``fs.defaultFS`` configuration property is always used when accessing ``wasb://`` URIs. This means that the ``wasb://`` container you want the 
GetHDFS processor to connect to must be hard coded in the HDFS ``core-site.xml`` config. This causes two problems. Firstly, it implies that we can only 
connect to one container in one account on Azure. Secondly, it causes problems when using NiFi on a server that is also running GeoMesa-Accumulo as 
the ``fs.defaultFS`` property needs to be set to the proper HDFS master NameNode. 

There are two ways to circumvent this problem. You can maintain a ``core-site.xml`` file for each container you want to access but this is not easily scalable 
or maintainable in the long run. The better option is to leave the default ``fs.defaultFS`` value in the HDFS ``core-site.xml`` file. We can then leverage 
the ``Hadoop Configuration Resources`` property in the GetHDFS processor. Normally you would set the ``Hadoop Configuration Resources`` property 
to the location of the ``core-site.xml`` and the ``hdfs-site.xml`` files. Instead we are going to create an additional file and include it last in the 
path so that it overwrites the value of the ``fs.defaultFS`` when the configuration object is built. To do this simply create a new xml file with an appropriate 
name (we suggest the name of the container). Insert the ``fs.defaultFS`` property into the file and set the value. 
 
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>wasb://container@accountName.blob.core.windows.net/</value>
    </property>
</configuration>
```

Reference
---------

For more information on setting up or using NiFi see the [Apache NiFi User Guide](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html)
