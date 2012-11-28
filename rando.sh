#!/bin/sh

cap=100
total=50
n=5

if ! [ -d rando ]; then
    mkdir rando
    cd rando
    touch $(seq 1 $cap | unsort | head -n $total)
else
    cd rando
fi

echo deleting
for x in $(ls|unsort|head -n $n); do
    echo $x
    rm $x
done

echo updating
for x in $(ls|unsort|head -n $n); do
    echo $x
    date > $x
done

echo making
for x in $(seq 1 $cap | unsort | head -n $n); do
    if ! [ -f $x ]; then
        echo $x
        touch $x
    fi
done

echo Total objects: $(ls|wc -l)