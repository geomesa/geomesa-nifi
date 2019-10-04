# GeoMesa NiFi

GeoMesa-NiFi allows you to ingest data into GeoMesa using the NiFi dataflow framework. Currently, the following
data stores are supported:

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

The nar contains bundled dependencies. To change the dependency versions, modify the version properties
(`<hbase.version>`, etc) in the `pom.xml` before building.

# Installation

To install the GeoMesa processors you will need to copy the nar files into the `lib` directory of your
NiFi installation. There are currently three nar files:

* `geomesa-nifi-controllers-api-nar-$VERSION.nar`
* `geomesa-nifi-controllers-nar-$VERSION.nar`
* `geomesa-nifi-processors-nar-$VERSION.nar`

For convenience, they are bundled into a `tar.gz` file, which is available through the
[GitHub Releases](https://github.com/geomesa/geomesa-nifi/releases) page or under
`geomesa-nifi-dist/target/geomesa-nifi-$VERSION-dist.tar.gz` after building from source.

For example, to download and install the nars from GitHub:

```bash
wget "https://github.com/geomesa/geomesa-nifi/releases/download/geomesa-nifi-$VERSION/geomesa-nifi-$VERSION-dist.tar.gz"
tar -xf geomesa-nifi-$VERSION-dist.tar.gz --directory $NIFI_HOME/lib/
````

Or, to install the nars after building from source:

```bash
tar -xf geomesa-nifi-dist/target/geomesa-nifi-$VERSION-dist.tar.gz --directory $NIFI_HOME/lib/
````

## Upgrading

In order to upgrade, replace the nar files with the latest versions. For version-specific changes,
see [Upgrade Guide](#upgrade-guide).

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
SftName                   | The SimpleFeatureType to use, if the SimpleFeatureType is already on the classpath (see below). Takes precedence over `SftSpec`
SftSpec                   | Define a SimpleFeatureType via TypeSafe config or GeoTools specification string
FeatureNameOverride       | Override the SimpleFeatureType name
ConverterName             | The converter to use, if the converter is already on the classpath (see below). Takes precedence over `ConverterSpec`
ConverterSpec             | Define a converter via TypeSafe config
ConverterErrorMode        | Override the converter error mode (`skip-bad-records` or `raise-errors`)
BatchSize                 | The number of flow files that will be processed in a single batch
FeatureWriterCaching      | Enable caching of feature writers between flow files, useful if flow files have a small number of records (see below)
FeatureWriterCacheTimeout | How often feature writers will be flushed to the data store, if caching is enabled

### Feature Writer Caching

Feature writer caching can be used to improve the throughput of processing many small flow files. Instead of a new
feature writer being created for each flow file, writers are cached and re-used between operations. If a writer is
idle for the configured timeout, then it will be flushed to the data store and closed.

Note that if feature writer caching is enabled, features that are processed may not show up in the data store
immediately. In addition, any features that have been processed but not flushed may be lost if NiFi shuts down
unexpectedly. To ensure data is properly flushed, stop the processor before shutting down NiFi.

Alternatively, NiFi's built-in `MergeContent` processor can be used to batch up small files.

### Defining SimpleFeatureTypes and Converters

The GeoMesa NiFi processors package a set of predefined SimpleFeatureType schema definitions and GeoMesa
converter definitions for popular data sources such as Twitter, GDelt and OpenStreetMaps.

The full list of provided sources can be found in the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/convert/premade/index.html).

For custom data sources, there are two ways of providing custom SFTs and converters:

#### Providing SimpleFeatureTypes and Converters on the Classpath

To bundle configuration in a jar file simply place your config in a file named ``reference.conf`` and place it **at
the root level** of a jar file:

```bash
jar cvf data-formats.jar reference.conf
```

You can verify your jar was built properly:

```bash
$ jar tvf data-formats.jar
     0 Mon Mar 20 18:18:36 EDT 2017 META-INF/
    69 Mon Mar 20 18:18:36 EDT 2017 META-INF/MANIFEST.MF
 28473 Mon Mar 20 14:49:54 EDT 2017 reference.conf
```

Add the jar file to `$NIFI_HOME/lib` to make it available on the classpath.

##### Defining SimpleFeatureTypes and Converters via the UI

You may also provide SimpleFeatureTypes and Converters directly in the Processor configuration via the NiFi UI.
Simply paste your TypeSafe configuration into the ``SftSpec`` and ``ConverterSpec`` property fields.

## PutGeoMesaAccumulo

The ``PutGeoMesaAccumulo`` processor is used for ingesting data into an Accumulo-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to Accumulo. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/accumulo/usage.html#accumulo-data-store-parameters)
for a description of the data store parameters.

### GeoMesa Configuration Service

The ``PutGeoMesaAccumulo`` plugin supports [NiFi Controller Services](http://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)
to manage common configurations. This allows the user to specify a single location to store the Accumulo connection parameters.
This allows you to add new processors without having to enter duplicate data.

To add the ``AccumuloDataStoreConfigControllerService`` access the ``Contoller Settings`` from NiFi global menu and navigate to the
``ControllerServices`` tab and click the ``+`` to add a new service. Search for the ``AccumuloDataStoreConfigControllerService``
and click add. Edit the new service and enter the appropriate values for the properties listed.

After configuring the service, select the appropriate service in the ``GeoMesa Configuration Service`` property
of your processor. When a controller service is selected the ``accumulo.zookeepers``, ``accumulo.instance.id``,
``accumulo.user``, ``accumulo.password`` and ``accumulo.catalog`` parameters are not required or used.

## PutGeoMesaHBase

The ``PutGeoMesaHBase`` processor is used for ingesting data into an HBase-backed GeoMesa data store. In
addition to the common properties defined above, it requires properties for connecting to HBase. See the
[GeoMesa documentation](https://www.geomesa.org/documentation/user/hbase/usage.html#hbase-data-store-parameters)
for a description of the data store parameters.

HBase generally requires an `hbase-site.xml` file to be on your classpath, in order to connect to your cluster.
The `PutGeoMesaHBase` processor provides two options to avoid this: you can use the `hbase.config.paths` property
to point to a file on the local file system, HDFS or S3, or you can simply paste the `<configuration>` into
the `hbase.config.xml` property.

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

The ``PutGeoTools`` processor is used for ingesting data into a GeoTools compatible data store. In
addition to the common properties defined above, it requires properties for connecting to the data store.

The `DataStoreName` property should match to the display name of the data store to use, for example `PostGIS`
to use a PostGIS data store. Additional data store parameters for the specific data store you are using can be
specified through NiFi dynamic properties.

Note that the dependencies for the requested data store must be available on the NiFi classpath.

## ConvertToGeoAvro

The ``ConvertToGeoAvro`` processor leverages GeoMesa's internal converter framework to convert features into Avro
and pass them along as a flow to be used by other processors in NiFi.

# Upgrade Guide

## 2.4.x

The GeoMesa processors have been refactored to support NiFi nar inheritance and as a first step towards supporting
Java 11. Any existing processors will continue to work under the older version, as long as you don't delete the old
GeoMesa nar file. However, you will need to create new processors in order to upgrade to 2.4.x.

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
