#!/usr/bin/env python

from argparse import ArgumentParser
import os
import sys

from db import finddb, logger

def main():
    ap = ArgumentParser(description="selectively ignore files for fts")

    ap.add_argument('--list', action='store_true', help="List current exclusions")
    ap.add_argument('--rm', nargs=1, help="remove a glob")
    ap.add_argument('--add', nargs=1, help="add a new glob")
    args = ap.parse_args()

    root, prefix, conn = finddb(os.getcwd())

    with conn:
        c = conn.cursor()

        if not (args.list or args.rm or args.add):
            ap.print_usage();
            sys.exit(1)

        if args.list:
            for (x,) in c.execute("""
                SELECT expression FROM exclusions;
            """):
                print x

        if args.rm:
            c.execute("DELETE FROM exclusions WHERE expression = ?", (args.rm[0],))

        if args.add:
            c.execute("INSERT INTO exclusions(expression) VALUES(?)", (args.add[0],))

        if args.rm or args.add:
            logger.warning("You may need to resync")

if __name__ == '__main__':
    main()
