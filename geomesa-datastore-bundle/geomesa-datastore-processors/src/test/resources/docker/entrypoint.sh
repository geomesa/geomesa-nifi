#! /usr/bin/env bash

# copy the flow files into nifi's conf dir, so that the originals are not modified
if [[ -f /flow.json.gz ]]; then
  cp /flow.json.gz /opt/nifi/nifi-current/conf/
elif [[ -f /flow.json ]]; then
  cp /flow.json /opt/nifi/nifi-current/conf/
  gzip /opt/nifi/nifi-current/conf/flow.json
fi
if [[ -f /logback.xml ]]; then
  cp /logback.xml /opt/nifi/nifi-current/conf/
fi

# lower the administrative yield deadline, to decrease test time when failing due to e.g. feature types not yet being available
sed -i s'/nifi.administrative.yield.duration=.*/nifi.administrative.yield.duration=1 sec/' /opt/nifi/nifi-current/conf/nifi.properties

backend="$(ls /opt/nifi/nifi-current/extensions/geomesa-*.nar | grep -v "datastore-services" | head -n1 | sed 's/geomesa-//')"
# these values are from the controller service UUIDs in the flow.json
controllerId=""
if [[ $backend =~ accumulo.* ]]; then
  controllerId="823a37c5-fda6-3e87-b250-c13d073baeeb"
elif [[ $backend =~ kafka.* ]]; then
  controllerId="980d4c03-f78b-3b68-a0a2-24c47b87e064"
  # confluentControllerId="638d3ed8-e9bb-3610-b0be-c5f43e84e008"
elif [[ $backend =~ fs.* ]]; then
  controllerId="272b191a-e2fe-334a-8d52-c513e05c124f"
elif [[ $backend =~ gt.* ]]; then
  controllerId="fbd8d02b-283c-3362-9d96-8b350af2d630"
elif [[ $backend =~ hbase.* ]]; then
  controllerId="c18303ac-76ab-3499-a4d0-dbbdf59a52d3"
elif [[ $backend =~ redis.* ]]; then
  controllerId="207c8fa2-06b4-372f-9cee-f7baa154a6ea"
fi

if [[ -n "$controllerId" ]]; then
  gunzip /opt/nifi/nifi-current/conf/flow.json.gz
  sed -i "s/\"DataStoreService\": \".*\"/\"DataStoreService\": \"${controllerId}\"/" /opt/nifi/nifi-current/conf/flow.json
  gzip /opt/nifi/nifi-current/conf/flow.json
else
  echo "Could not find GeoMesa nar: $backend"
  exit 1
fi

# delegate to the normal nifi entrypoint
exec /opt/nifi/scripts/start.sh
