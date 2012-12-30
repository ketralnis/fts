#!/bin/sh -e

# add fts and rando.py to the $PATH
export PATH=$PATH:$(pwd)

numdirs=100
initialcount=1000
minfsize=500
maxfsize=1000

smallcycle=5
bigcycle=150
bigsearches=50

if [ -d bigrando ]; then
    tmpd=$(pwd)/bigrando
    cd $tmpd
    echo using existing bigrando of $(find . -type f | wc -l) files in $(du -sh .)
    echo deleting old .fts.db
    rm -f ./.fts.db

else

    tmpd=$(mktemp -d -t $(basename $0).$$)
    trap "rm -fr $tmpd" EXIT

    cd $tmpd

    echo making large directory $tmpd
    time for x in $(seq 1 $numdirs); do
        mkdir $x
        cd $
        rando.py --initialcount $initialcount --mxinfsize $minfsize --maxfsize $maxfsize
        cd ..
    done
    echo created $(find . -type f | wc -l) files in $(du -sh .)
fi

echo creating initial index
time fts --init --sync
ls -lh .fts.db

echo sync after no updates
time fts --sync

echo sync after minor updates "($smallcycle)"
for x in $(seq 1 $numdirs); do
    cd $x
    rando.py --initialcount $initialcount --minfsize $minfsize --maxfsize $maxfsize --cycle $smallcycle
    cd ..
done
time fts --sync

echo sync after major updates "($bigcycle)"
for x in $(seq 1 $numdirs); do
    cd $x
    rando.py --initialcount $initialcount --minfsize $minfsize --maxfsize $maxfsize --cycle $bigcycle
    cd ..
done
time fts --sync

cat /usr/share/dict/words | unsort | head -n $bigsearches > searchlist

echo time to do $bigsearches separate searches
time cat searchlist | xargs -n1 fts > /dev/null

echo time to do $bigsearches separate searches in groups of 10
time cat searchlist | xargs -n10 fts > /dev/null

echo time to do $bigsearches separate regex searches
time cat searchlist | xargs -n1 fts --re > /dev/null

echo time to do $bigsearches separate regex searches in groups of 10
time cat searchlist | xargs -n10 fts --re > /dev/null

