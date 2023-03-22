#! /usr/bin/env bash
# runs a nifi docker at https://localhost:8443/
# requires nars to be built, first
dir="$(cd "`dirname "$0"`/.."; pwd)"
if [[ -z "$1" ]]; then
  echo "Usage: list-nar.sh <nar-to-list>"
  echo "Available NARs:"
  find $dir -name "geomesa-*-nar" -type d | sort | sed 's|.*/geomesa-\([a-z0-9]\+\)-nar|  \1|'
  exit 1
fi

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

nar="$(find $dir/geomesa-* -name "geomesa-$1*.nar")"

checkNar "$nar" "$1"
echo "$nar"
unzip -l "$nar" | grep dep | awk '{print $4}' | sort
