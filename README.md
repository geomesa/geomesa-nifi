# GeoMesa NiFi

GeoMesa-NiFi allows you to ingest data into GeoMesa using the NiFi dataflow framework. Currently, the following data stores are supported:

* Accumulo
* HBase
* Kafka
* FileSystem (HDFS, S3)
* Redis
* Arbitrary GeoTools data stores (e.g. Postgis)

# Building from Source

To build, simply clone and build with maven:

```bash
git clone git@github.com:geomesa/geomesa-nifi.git
cd geomesa-nifi
mvn clean install
```

## Dependency Versions

The nar contains bundled dependencies. To change the dependency versions, modify the `<accumulo.version>`,
`<hbase.version>` and/or `<kafka.version>` properties in the `pom.xml` before building.

# Installation

To install the GeoMesa processors you will need to copy the nar file into the ``lib`` directory of your
NiFi installation. There is currently a single nar file, in the `geomesa-nifi-nar` module.

For example, to install the nar after building from source:

```bash
cp geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-$VERSION.nar $NIFI_HOME/lib/
````

## Upgrading

In order to upgrade, replace the `geomesa-nifi-nar` files with the latest version. For version-specific changes,
see [Upgrade Path](#upgrade-path).

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
* ``PutGeoMesaKafka`` - Ingest data into GeoMesa Kafka
* ``PutGeoMesaFileSystem`` - Ingest data into a distributed file system like HDFS, S3 or WASB
* ``PutGeoMesaRedis`` - Ingest data into GeoMesa Redis
* ``PutGeoTools`` - Ingest data into an arbitrary GeoTools Datastore
* ``ConvertToGeoAvro`` - Use a GeoMesa converter to create GeoAvro

## Common Processor Configurations

All geomesa-nifi processors share some common configuration properties, that define how input data is converted
into GeoTools SimpleFeatures for ingest. To use a processor, first add it to the workspace and open the properties
tab of its configuration. Common properties are as follows:

Property                  | Description
---                       | ---
Mode                      | Ingest arbitrary files with `Converter` mode, or GeoAvro with `Avro` mode
SftName                   | The SimpleFeatureType to use, if the SimpleFeatureType is already on the classpath (see above). Takes precedence over `SftSpec`
SftSpec                   | Define a SimpleFeatureType via TypeSafe config or GeoTools specification string
FeatureNameOverride       | Override the SimpleFeatureType name
ConverterName             | The converter to use, if the converter is already on the classpath (see above). Takes precedence over `ConverterSpec`
ConverterSpec             | Define a converter via TypeSafe config
ConverterErrorMode        | Override the converter error mode (`skip-bad-records` or `raise-errors`)
BatchSize                 | The number of flow files that will be processed in a single batch
FeatureWriterCaching      | Enable caching of feature writers between flow files, useful if flow files have a small number of records
FeatureWriterCacheTimeout | How often feature writers will be flushed to the data store, if caching is enabled

### Feature Writer Caching

Feature writer caching can be used to improve the throughput of processing many small flow files. Instead of a new
feature writer being created for each flow file, writers are cached and re-used between operations. If a writer is
idle for the configured timeout, then it will be flushed to the data store and closed.

Note that if feature writer caching is enabled, features that are processed may not show up in the data store
immediately. In addition, any features that have been processed but not flushed may be lost if NiFi shuts down
unexpectedly. To ensure data is properly flushed, shut down the processor before stopping NiFi.

An alternative to feature writer caching is to use NiFi's built-in `MergeContent` processor to batch up small files.

## PutGeoMesaAccumulo

The ``PutGeoMesaAccumulo`` processor is used for ingesting data into an Accumulo-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to Accumulo. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/accumulo/usage.html#accumulo-data-store-parameters)
for a description of the data store parameters.

### GeoMesa Configuration Service

The ``PutGeoMesaAccumulo`` plugin supports [NiFi Controller Services](http://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)
to manage common configurations. This allows the user to specify a single location to store the Accumulo connection parameters.
This allows you to add new processors without having to enter duplicate data.

To add the ``GeomesaConfigControllerService`` access the ``Contoller Settings`` from NiFi global menu and navigate to the
``ControllerServices`` tab and click the ``+`` to add a new service. Search for the ``GeomesaConfigControllerService``
and click add. Edit the new service and enter the appropriate values for the properties listed.

To use this feature, after configuring the service, select the appropriate Geomesa Config Controller Service from the drop down
of the ``GeoMesa Configuration Service`` property. When a controller service is selected the ``accumulo.zookeepers``,
``accumulo.instance.id``, ``accumulo.user``, ``accumulo.password`` and ``accumulo.catalog`` parameters are not required or used.

## PutGeoMesaHBase

The ``PutGeoMesaHBase`` processor is used for ingesting data into an HBase-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to HBase. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/hbase/usage.html#hbase-data-store-parameters)
for a description of the data store parameters.

Of note, the `hbase.config.paths` parameter is required for the NiFi processor, as generally you would not want
to install your `hbase-site.xml` in your NiFi classpath. Instead, you can point to a location on the local file
system, HDFS or S3.

## PutGeoMesaKafka

The ``PutGeoMesaKafka`` processor is used for ingesting data into a Kafka-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to Kafka. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/kafka/usage.html#kafka-data-store-parameters)
for a description of the data store parameters.

## PutGeoMesaFileSystem

The ``PutGeoMesaFileSystem`` processor is used for ingesting data into a file-system-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties defining the file system. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/filesystem/usage.html#filesystem-data-store-parameters)
for a description of the data store parameters.

## PutGeoMesaRedis

The ``PutGeoMesaRedis`` processor is used for ingesting data into a Redis-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to Redis. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/redis/usage.html#redis-data-store-parameters)
for a description of the data store parameters.

## PutGeoTools

The ``PutGeoTools`` processor is used for ingesting data into a GeoTools compatible datastore. In
addition to the common properties defined above, it requires properties for connecting to the data store.

The `DataStoreName` property should match to the display name of the data store to use, for example `PostGIS`
to use a PostGIS data store. Additional data store parameters for the specific data store you are using can be
specified through NiFi dynamic properties.

## ConvertToGeoAvro

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal converter framework to convert features into Avro
and pass them along as a flow to be used by other processors in NiFi.

# Upgrade Path

## 1.3.x to 1.4.x

The `PutGeoMesaKafka_09` and `PutGeoMesaKafka_10` processors have been merged into a single `PutGeoMesaKafka`
processor that will work with both Kafka 09 and 10.

The configuration parameters for the following processors have changed:

* ``PutGeoMesaAccumulo``
* ``PutGeoMesaHBase``
* ``PutGeoMesaKafka``

See [Processors](#processors) for more details.

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
