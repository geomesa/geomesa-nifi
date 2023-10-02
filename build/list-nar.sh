#! /usr/bin/env bash
# runs a nifi docker at https://localhost:8443/
# requires nars to be built, first
dir="$(cd "$(dirname "$0")/.." || exit 1; pwd)"

# find available nars
mapfile -t nars < <( find "$dir"/geomesa-* -name "geomesa-*-nar" -type d | sort | sed 's|.*/geomesa-\([a-z0-9-]\+\)-nar|\1|' )

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

if [[ -n "$1" ]]; then
  declare -a filtered=()
  for nar in "${nars[@]}"; do
    echo "$nar" | grep -q "$1" && filtered+=("$nar")
  done
  if [[ ${#filtered[@]} -eq 0 ]]; then
    echo "$1 is not a valid nar - available nars:"
    for nar in "${nars[@]}"; do
      echo "  $nar"
    done
    exit 1
  else
    nars=( "${filtered[@]}" )
  fi
fi

for nar in "${nars[@]}"; do
  file="$(find "$dir"/geomesa-* -name "geomesa-${nar}-nar_*.nar")"
  checkNar "$file" "$nar"
  echo "$nar::"
  unzip -l "$file" | grep bundled-dependencies | sed 's|.*bundled-dependencies/||' | grep -v '^$' | sort
  echo ""
done
