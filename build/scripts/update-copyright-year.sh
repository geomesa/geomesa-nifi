#!/usr/bin/env bash

to="$(date +%Y)"
from="$((to - 1))"
dir="$(dirname "$0")/../.."

sed -i "s|<copyright.year>$from</copyright.year>|<copyright.year>$to</copyright.year>|" "$dir/pom.xml"

for file in $(find "$dir" -name '*.scala') $(find "$dir" -name '*.java'); do
  sed -i \
    -e "s/Copyright (c) 2015-$from General Atomics Integrated Intelligence, Inc\./Copyright (c) 2015-$to General Atomics Integrated Intelligence, Inc./" \
    "$file"
done
