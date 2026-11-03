#!/bin/bash
# usage: tla/lease/run.sh <cfg>|all   regenerates the .cfg files, refuses unknown names, runs TLC via tla/run2.sh
set -euo pipefail
cd "$(dirname "$0")/.."
python3 gencfg.py lease
if [ "$1" = all ]; then names=$(cat lease/cfglist.txt); else names="$*"; fi
for n in $names; do
  python3 -c "import sys; ns={}; exec(open('lease/configs.py').read(), ns); ns['get'](sys.argv[1])" "$n"
  ./run2.sh lease LeaseCommit "$n"
done
