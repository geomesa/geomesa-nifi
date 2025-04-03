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

# allow debugging outside localhost
sed -i 's/java.arg.debug=.*/java.arg.debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8000/' /opt/nifi/nifi-current/conf/bootstrap.conf

if [[ -n "$NIFI_WEB_HTTP_PORT" ]]; then
  sed -i "s/nifi.web.http.host=.*/nifi.web.http.host=${NIFI_WEB_HTTP_HOST:-}/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.web.http.port=.*/nifi.web.http.port=$NIFI_WEB_HTTP_PORT/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.web.https.host=.*/nifi.web.https.host=/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.web.https.port=.*/nifi.web.https.port=/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.remote.input.http.enabled=.*/nifi.remote.input.http.enabled=false/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.security.keystore=.*/nifi.security.keystore=/" /opt/nifi/nifi-current/conf/nifi.properties
  sed -i "s/nifi.security.truststore=.*/nifi.security.truststore=/" /opt/nifi/nifi-current/conf/nifi.properties
  # delete https config from the entrypoint, as there's no way to override it
  sed -i '/nifi.web.https.port/d' /opt/nifi/scripts/start.sh
  sed -i '/nifi.web.https.host/d' /opt/nifi/scripts/start.sh
fi

# delegate to the normal nifi entrypoint
exec /opt/nifi/scripts/start.sh
