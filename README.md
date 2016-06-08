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
