#!/bin/sh -e

echo creating data
./rando.sh | sed 's/^/    /'

echo deleting old data
rm -f .fts.db

echo creating initial db
python ./ftsinit.py

echo test search
python ./ftssearch.py $(cat $(ls rando/* | unsort | head -n 1) | unsort | head -n 1)

echo more data
./rando.sh | sed 's/^/    /'

echo test sync
python ./ftssync.py

echo mroe data
./rando.sh | sed 's/^/    /'
pushd rando > /dev/null
echo sync from in rando
python ../ftssync.py
echo search from in rando
python ../ftssearch.py $(cat $(ls | unsort | head -n 1) | unsort | head -n 1)
popd > /dev/null

