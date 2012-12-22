#!/usr/bin/env pypy-c

import argparse
import random
import os
import os.path

wordsfile='/usr/share/dict/words'

ap = argparse.ArgumentParser()
ap.add_argument('--initialcount', default=50,     type=int) # how many files to create in a new directory
ap.add_argument('--minfsize',     default=5,      type=int)
ap.add_argument('--maxfsize',     default=15,     type=int)
ap.add_argument('--cycle',        default=5,      type=int) # how many files to create/updated/delete
ap.add_argument('--cap',          default=100000, type=int) # filenames are in the range 1..cap

args = ap.parse_args()

# leave the newlines on
words = list(open(wordsfile))

def rslice(from_=args.minfsize, to=args.maxfsize):
    t1 = min(from_, len(words)-1)
    t2 = min(to, len(words), t1)
    return random.sample(words, random.randint(t1, t2))

if not os.path.exists('rando'):
    os.mkdir('rando')
    os.chdir('rando')

    for fname in xrange(args.initialcount):
        with open(str(fname), 'w') as f:
            f.write(''.join(rslice()))
else:
    os.chdir('rando')

# make some new files
for x in xrange(args.cycle):
    fname = str(random.randint(1, args.cap))
    with open(fname, 'w') as f:
        f.write(''.join(rslice()))

# delete some
for x in random.sample(os.listdir('.'), args.cycle):
    os.unlink(x)

# update some
for x in random.sample(os.listdir('.'), args.cycle):
    with open(x, 'w') as f:
        f.write(''.join(rslice()))
