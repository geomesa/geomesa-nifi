#! /usr/bin/env bash
# runs a nifi docker at https://localhost:8443/
# requires nars to be built, first
dir="$(cd "$(dirname "$0")/.." || exit; pwd)"

nifiVersion="$(grep '<nifi.version>' "$dir/pom.xml" | sed 's|.*<nifi.version>\(.*\)</nifi.version>.*|\1|')"

function usage() {
  echo "Usage: run-nifi.sh <options> <nar-to-mount>"
  echo "Options:"
  echo "  -v <version>  NiFi version to run (default $nifiVersion)"
  echo "Available back-end NARs:"
  find "$dir" -name "geomesa-*-nar" -type d | grep -v datastore-services | sort | sed 's|.*/geomesa-\([a-z0-9]\+\)-nar|  \1|'
}

function checkNar() {
  local nar="$1"
  local desc="$2"
  if [[ -z "$nar" ]]; then
    echo "No $desc nar found... try building with maven"
    exit 1
  elif [[ $(echo "$nar" | wc -l) -gt 1 ]]; then
    echo -e "Found multiple nars: \n$nar"
    exit 2
  fi
}

while getopts ":v:" opt; do
  case $opt in
    v)
      nifiVersion="$OPTARG"
      echo "nifiVersion=$nifiVersion"
      shift 2
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      help
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      help
      exit 1
      ;;
  esac
done

backend="$1"
if [[ -z "$backend" ]]; then
  usage
  exit 1
fi

nar="$(find "$dir"/geomesa-* -name "geomesa-$backend*.nar")"
datastoreNar="$(find "$dir"/geomesa-datastore-bundle/geomesa-datastore-services-nar/target -name "geomesa*.nar")"
servicesApiNar="$(find "$dir"/geomesa-datastore-bundle/geomesa-datastore-services-api-nar/target -name "geomesa*.nar")"

checkNar "$nar" "$backend"
checkNar "$datastoreNar" "datastore-services"
checkNar "$servicesApiNar" "datastore-services-api"

mkdir -p "$dir/build/docker/ingest"

if ! docker network ls | grep -q geomesa; then
  echo "Creating docker network"
  docker network create geomesa
fi

# these values are from the controller service UUIDs in the flow.json
controllerId=""
if [[ $backend =~ accumulo.* ]]; then
  controllerId="823a37c5-fda6-3e87-b250-c13d073baeeb"
elif [[ $backend = 'kafka' ]]; then
  controllerId="980d4c03-f78b-3b68-a0a2-24c47b87e064"
  # confluentControllerId="638d3ed8-e9bb-3610-b0be-c5f43e84e008"
elif [[ $backend = 'fs' ]]; then
  controllerId="272b191a-e2fe-334a-8d52-c513e05c124f"
elif [[ $backend = 'gt' ]]; then
  controllerId="fbd8d02b-283c-3362-9d96-8b350af2d630"
elif [[ $backend = 'hbase' ]]; then
  controllerId="c18303ac-76ab-3499-a4d0-dbbdf59a52d3"
elif [[ $backend = 'redis' ]]; then
  controllerId="207c8fa2-06b4-372f-9cee-f7baa154a6ea"
fi

if [[ -n "$controllerId" ]]; then
  sed -i "s/\"DataStoreService\": \".*\"/\"DataStoreService\": \"${controllerId}\"/" "$dir/build/docker/flow.json"
else
  echo "WARN: No controller specified for $backend"
fi

echo "Running NiFi $nifiVersion with $(basename "$nar")..."
echo ""

docker run --rm \
  --name nifi \
  --network geomesa \
  -p 8081:8081 -p 8000:8000 \
  -e NIFI_WEB_HTTP_HOST=0.0.0.0 \
  -e NIFI_WEB_HTTP_PORT=8081 \
  -e SINGLE_USER_CREDENTIALS_USERNAME=nifi \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=nifipassword \
  -e NIFI_SENSITIVE_PROPS_KEY=supersecretkey \
  -e NIFI_JVM_DEBUGGER=true \
  -v "$nar:/opt/nifi/nifi-current/extensions/$(basename "$nar"):ro" \
  -v "$datastoreNar:/opt/nifi/nifi-current/extensions/$(basename "$datastoreNar"):ro" \
  -v "$servicesApiNar:/opt/nifi/nifi-current/extensions/$(basename "$servicesApiNar"):ro" \
  -v "$dir/build/docker/flow.json:/flow.json:ro" \
  -v "$dir/build/docker/logback.xml:/logback.xml:ro" \
  -v "$dir/build/docker/entrypoint.sh:/entrypoint.sh:ro" \
  -v "$dir/build/docker/ingest:/ingest:ro" \
  --entrypoint "/entrypoint.sh" \
  "apache/nifi:$nifiVersion"
