# GeoMesa Ingest with Nifi

This project contains three processors:
* PutGeoMesa - Ingest data into GeoMesa with a GeoMesa converter or from geoavro
* PutGeoTools - Ingest data into an arbitrary GeoTools Datastore based on parameters using a GeoMesa converter or avro
* ConvertToGeoAvro - Use a GeoMesa converter to create geoavro

To Setup...

Download and untar nifi 0.4.1 from the nifi website... https://www.apache.org/dyn/closer.lua?path=/nifi/0.4.1/nifi-0.4.1-bin.tar.gz

clone and build this repo...

Copy the nar file (niagara files archive) geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-0.4.0-SNAPSHOT.nar into the $NIFI_HOME/lib/ directory

For geomesa SFTs and configs install the gm-data-all resource bundle from the 
[geomesa/gm-data](https://github.com/geomesa/gm-data) project. It contains converters and SFT specs for common types 
such as twitter, geolife, osm-gpx, etc.

start up nifi
cd $NIFI_HOME
bin/nifi.sh start

tail -f logs/nifi-app.log

Go to http://localhost:8080/nifi

Drag a GeoMesaIngestProcessor down...fill out the accumulo instance, etc...for sft and converter try "example-csv" and 
copy an example csv from the geomesa-tools/examples/ingest/csv folder into a temp dir...use the getfile processor to 
pick it up and route it to the PutGeoMesa...

## GeoMesa Kafka Processor

The GeoMesa kafka processor allows for live layers. To use it you'll need a kafka broker set up...for localhost
testing just download it and do this:
   
   tar xvf kafka_2.11-0.9.0.1.tgz
   cd kafka_2.11-0.9.0.1/
   bin/zookeeper-server-start.sh config/zookeeper.properties &
   bin/kafka-server-start.sh config/server.properties &
   
Then you can use ```localhost:9092``` and ```localhost:2181``` as your brokers and zookeeper config string, respectively
in your kafka nifi ingest processor.