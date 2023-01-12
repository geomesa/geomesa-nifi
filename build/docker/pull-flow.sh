#! /usr/bin/env bash

dir="$(cd "`dirname "$0"`"; pwd)"
image="$(docker ps | grep nifi | awk '{ print $1 }')"

if [[ -z "$image" ]]; then
  echo "Error: could not find running NiFi docker instance"
  exit 1
fi

docker cp $image:/opt/nifi/nifi-current/conf/flow.json.gz $dir/flow.json.gz
docker cp $image:/opt/nifi/nifi-current/conf/flow.xml.gz $dir/flow.xml.gz
