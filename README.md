# GeoMesa Ingest with Nifi

This project contains an example GeoMesaIngestProcessor that can be configured with runtime SFT and Converter Configs.

To Setup...

Download and untar nifi 0.4.1 from the nifi website... https://www.apache.org/dyn/closer.lua?path=/nifi/0.4.1/nifi-0.4.1-bin.tar.gz

clone and build this repo...

Copy the nar file (niagara files archive) geomesa-nifi/geomesa-nifi-nar/target/geomesa-nifi-nar-0.0.2-SNAPSHOT.nar into the $NIFI_HOME/lib/ directory

Copy a sample resources bundle (providing sfts and config on the command line) geomesa-nifi/geomesa-nifi-resources/target/geomesa-nifi-resources-0.0.2-SNAPSHOT.jar into the $NIFI_HOME/lib directory as well. Add any types you would like to the application.conf file there. This works exactly like the geomesa-tools runtime SFTs and Converter configs.

start up nifi
cd $NIFI_HOME
bin/nifi.sh start

tail -f logs/nifi-app.log

Go to http://localhost:8080/nifi

Drag a GeoMesaIngestProcessor down...fill out the accumulo instance, etc...for sft and converter try "example-csv" and copy an example csv from the geomesa-tools/examples/ingest/csv folder into a temp dir...use the getfile processor to pick it up and route it to the GeoMesaIngestProcessor...


