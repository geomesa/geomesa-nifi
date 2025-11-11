#! /usr/bin/env bash
# checks nars for duplicate jars inherited from the parent
# requires nars to be built with maven, first
dir="$(cd "$(dirname "$0")/../.." || exit 1; pwd)"

set -e
set -o pipefail

# list out existing nars
if ! "${dir}"/build/scripts/list-nar.sh > nars.list.tmp; then
  echo "Error listing nars:"
  cat nars.list.tmp
  rm nars.list.tmp
  exit 1
fi

nifi_version="$(grep '<nifi.version>' "${dir}"/pom.xml | sed 's|.*<nifi\.version>\([0-9.]\+\)</nifi\.version>|\1|')"

# note: this is our current parent nar hierarchy, but may be subject to change
for nar in nifi-standard-services-api-nar nifi-aws-service-api-nar nifi-standard-shared-nar; do
  docker run --rm --entrypoint bash -it "apache/nifi:${nifi_version}" -c "unzip -l lib/${nar}-${nifi_version}.nar" | grep 'bundled-dependencies/' | sed -e 's|.*/bundled-dependencies/||' -e 's/\r//' | sort -u >> parent-nars.list.tmp
done

found_some=""
while IFS= read -r dep; do
  if [[ -n "${dep}" ]]; then
    if grep -q "^${dep%-*}" nars.list.tmp; then
      echo "${dep} potential duplicates:"
      grep "^${dep%-*}" nars.list.tmp
      echo ""
      found_some="true"
    fi
  fi
done < parent-nars.list.tmp

if [[ -z "${found_some}" ]]; then
  echo "No duplicates detected"
fi

rm nars.list.tmp parent-nars.list.tmp
