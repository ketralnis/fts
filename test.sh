#!/bin/sh -e

./rando.sh
rm -f .fts.db
python ./ftsinit.py
python ./ftssearch.py $(cat $(ls rando/* | unsort | head -n 1) | unsort | head -n 1) | xargs cat
