#! /usr/bin/env bash

dir="$(cd "$(dirname "$0")" || exit; pwd)"
image="$(docker ps | grep nifi | awk '{ print $1 }')"

if [[ -z "$image" ]]; then
  echo "Error: could not find running NiFi docker instance"
  exit 1
fi

docker cp "$image:/opt/nifi/nifi-current/conf/flow.json.gz" "$dir/flow.json.gz"
gunzip -f "$dir/flow.json.gz"
jq . "$dir/flow.json" > "$dir/flow.json.tmp" && mv "$dir/flow.json.tmp" "$dir/flow.json"
if [[ -f "$dir/flow.json.bak" ]]; then
  rm "$dir/flow.json.bak" # prevent run script from erasing the newly pulled file
fi
