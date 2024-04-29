#! /usr/bin/env bash

# copy the flow files into nifi's conf dir, so that the originals are not modified
if [[ -f /flow.json.gz ]]; then
  cp /flow.json.gz /opt/nifi/nifi-current/conf/
elif [[ -f /flow.json ]]; then
  cp /flow.json /opt/nifi/nifi-current/conf/
  gzip /opt/nifi/nifi-current/conf/flow.json
fi
if [[ -f /flow.xml.gz ]]; then
  cp /flow.xml.gz /opt/nifi/nifi-current/conf/
elif [[ -f /flow.xml ]]; then
  cp /flow.xml /opt/nifi/nifi-current/conf/
  gzip /opt/nifi/nifi-current/conf/flow.xml
fi
if [[ -f /logback.xml ]]; then
  cp /logback.xml /opt/nifi/nifi-current/conf/
fi

# allow debugging outside localhost
sed -i 's/java.arg.debug=.*/java.arg.debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8000/' /opt/nifi/nifi-current/conf/bootstrap.conf

# delegate to the normal nifi entrypoint
exec /opt/nifi/scripts/start.sh
