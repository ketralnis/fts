#!/bin/bash -e

wordsfile=/usr/share/dict/words

# how many files to create when the rando directory doesn't exist
initialcount=50

# how many files to create/destroy/update on each cycle
cycle=5

# the filenames are numbers in the range 1..$cap
cap=10000

function rslice {
    python -c "
import sys
import random
lines = filter(None, open('$wordsfile'))
t1 = min(5, len(lines))
t2 = min(15, len(lines))
sample = random.sample(lines, random.randint(t1, t2))
sys.stdout.write(''.join(sample)) # they already have newlines in them
"
}

function rsample {
    unsort | head -n "$1"
}

if ! [ -d rando ]; then
    echo creating directory
    mkdir rando
    cd rando
    for x in $(seq 1 $cap | rsample $initialcount); do
        rslice > $x
    done
else
    cd rando
fi

(echo deleting
for x in $(ls | rsample $cycle); do
    echo $x
    rm $x
done) | xargs echo

(echo updating
for x in $(ls | rsample $cycle); do
    echo $x
    rslice > $x
done) | xargs echo

(echo making
for x in $(seq 1 $cap | rsample $cycle); do
    if ! [ -f $x ]; then
        echo $x
        rslice > $x
    fi
done) | xargs echo

echo Total objects: $(ls|wc -l)