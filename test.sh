#!/bin/sh -e

export PATH=$PATH:$(pwd)

echo creating data
./rando.sh | sed 's/^/    /'

echo deleting old data
rm -f .fts.db

echo creating initial db
ftsinit

search=$(cat $(ls rando/* | unsort | head -n 1) | unsort | head -n 1)
echo test search "($search)"
fts "$search"

echo more data
./rando.sh | sed 's/^/    /'

echo test sync
ftssync

echo more data
./rando.sh | sed 's/^/    /'
pushd rando > /dev/null
    echo sync from in rando
    ftssync
    search=$(cat $(ls | unsort | head -n 1) | unsort | head -n 1)
    echo search from in rando "($search)"
    fts "$search"
popd > /dev/null

