#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cd "$(dirname "$0")/../.." || exit

usage() {
  echo "Usage: $(basename "$0") [-h|--help]
where :
  -h| --help Display this help text
" 1>&2
  exit 1
}

if [[ ($# -ne 0) ]]; then
  usage
fi

readTagFromRelease() {
  grep "^scm.tag=" release.properties | head -n1 | sed "s/scm.tag=//"
}

readVersionFromPom() {
  grep '^    <version>' "pom.xml$1" | head -n1 | sed -E 's|.*<version>(.*)</version>.*|\1|'
}

copyReleaseArtifacts() {
  while IFS= read -r -d '' file; do
    pushd "$(dirname "$file")" >/dev/null
    sha256sum "$(basename "$file")" > "$(basename "$file").sha256"
    popd >/dev/null
    mv "$file"{,.sha256,.asc} "$RELEASE"
  done < <(find geomesa-* -name "*-$RELEASE.nar" -print0)
}

JAVA_VERSION="$(mvn help:evaluate -Dexpression=jdk.version -q -DforceStdout)"
if ! [[ $(java -version 2>&1 | head -n 1 | cut -d'"' -f2) =~ ^$JAVA_VERSION.* ]]; then
  echo "Error: invalid Java version - Java $JAVA_VERSION required"
  exit 1
fi

if ! [[ $(which gpg) ]]; then
  echo "Error: gpg executable not found (required for signed release)"
  exit 1
fi

# get current branch we're releasing off
BRANCH="$(git branch --show-current)"

# use the maven release plugin to make the pom changes and tag
mvn release:prepare \
  -DautoVersionSubmodules=true \
  -Darguments="-DskipTests -Dmaven.javadoc.skip=true -Ppython" \
  -Ppython

TAG="$(readTagFromRelease)"

# clean up leftover release artifacts
mvn release:clean

# deploy to maven central
git checkout "$TAG"
RELEASE="$(readVersionFromPom "")"
mkdir -p "$RELEASE"

mvn clean deploy -Pcentral,python -DskipTests | tee build_2.12.log
copyReleaseArtifacts

./build/scripts/change-scala-version.sh 2.13
mvn clean deploy -Pcentral,python -DskipTests | tee build_2.13.log
copyReleaseArtifacts

# reset pom changes
./build/scripts/change-scala-version.sh 2.12

# go back to original branch
git checkout "$BRANCH"
