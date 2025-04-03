#! /usr/bin/env bash
# lists out the jars in each of our nars
# requires nars to be built with maven, first
dir="$(cd "$(dirname "$0")/../.." || exit 1; pwd)"

# find available nars
mapfile -t nars < <( mvn -f "$dir" -q -am exec:exec -Dexec.executable="pwd" -T8 | grep -e '-nar$' | sort )

function checkNar() {
  local nar="$1"
  if [[ -z "$nar" ]]; then
    echo "No ${nar##*/} nar found... try building with maven"
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
  file="$(find "$nar"/target -name '*.nar')"
  checkNar "$file"
  echo "${nar##*/}::"
  unzip -l "$file" | grep bundled-dependencies | sed 's|.*bundled-dependencies/||' | grep -v '^$' | sort
  echo ""
done
