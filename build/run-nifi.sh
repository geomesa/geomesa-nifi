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

if [[ -z "$1" ]]; then
  usage
  exit 1
fi

nar="$(find "$dir"/geomesa-* -name "geomesa-$1*.nar")"
datastoreNar="$(find "$dir"/geomesa-datastore-bundle/geomesa-datastore-services-nar/target -name "geomesa*.nar")"
servicesApiNar="$(find "$dir"/geomesa-datastore-bundle/geomesa-datastore-services-api-nar/target -name "geomesa*.nar")"

checkNar "$nar" "$1"
checkNar "$datastoreNar" "datastore-services"
checkNar "$servicesApiNar" "datastore-services-api"

mkdir -p "$dir/build/docker/ingest"

echo "Running NiFi $nifiVersion with $(basename "$nar")..."
echo ""

docker run --rm \
  --network host \
  -e NIFI_WEB_HTTP_HOST=0.0.0.0 \
  -e NIFI_WEB_HTTP_PORT=8080 \
  -e "NIFI_WEB_PROXY_HOST=$(nslookup "$(hostname)" | grep Name | head -n1 | awk '{ print $2 }'):8080" \
  -e SINGLE_USER_CREDENTIALS_USERNAME=nifi \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=nifipassword \
  -e NIFI_SENSITIVE_PROPS_KEY=supersecretkey \
  -e NIFI_JVM_DEBUGGER=true \
  -v "$nar:/opt/nifi/nifi-current/extensions/$(basename "$nar"):ro" \
  -v "$datastoreNar:/opt/nifi/nifi-current/extensions/$(basename "$datastoreNar"):ro" \
  -v "$servicesApiNar:/opt/nifi/nifi-current/extensions/$(basename "$servicesApiNar"):ro" \
  -v "$dir/build/docker/flow.json.gz:/flow.json.gz:ro" \
  -v "$dir/build/docker/logback.xml:/logback.xml:ro" \
  -v "$dir/build/docker/entrypoint.sh:/entrypoint.sh:ro" \
  -v "$dir/build/docker/ingest:/ingest:ro" \
  --entrypoint "/entrypoint.sh" \
  "apache/nifi:$nifiVersion"
