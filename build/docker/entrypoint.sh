#! /usr/bin/env bash

# copy the flow files into nifi's conf dir, so that the originals are not modified
if [[ -f /flow.json.gz ]]; then
  cp /flow.json.gz /opt/nifi/nifi-current/conf/
fi
if [[ -f /logback.xml ]]; then
  cp /logback.xml /opt/nifi/nifi-current/conf/
fi

# delegate to the normal nifi entrypoint
exec /opt/nifi/scripts/start.sh
