#!/usr/bin/env python

from argparse import ArgumentParser
import os
import sys

from ftsdb import finddb, logger

def main():
    ap = ArgumentParser(description="selectively ignore files for fts")

    ap.add_argument('--list', action='store_true', help="List current exclusions")
    ap.add_argument('--rm', nargs=1, help="remove a glob")
    ap.add_argument('--re', nargs=1, help="add a new regex exclusion")
    ap.add_argument('--glob', nargs=1, help="add a new glob exclusion")
    ap.add_argument('--simple', nargs=1, help="add a new simple exclusion")
    args = ap.parse_args()

    root, prefix, conn = finddb(os.getcwd())

    with conn:
        c = conn.cursor()

        if not (args.list or args.rm or args.re or args.glob or args.simple):
            ap.print_usage();
            sys.exit(1)

        if args.list:
            for (_id, typ, expression) in c.execute("""
                SELECT id, type, expression FROM exclusions;
            """):
                print '\t'.join(map(str, (_id, typ, expression)))

        if args.rm:
            c.execute("DELETE FROM exclusions WHERE id = ?", (int(args.rm[0]),))

        if args.re:
            c.execute("INSERT INTO exclusions(id, type, expression) VALUES(NULL, 're', ?)", (args.re[0],))

        if args.glob:
            c.execute("INSERT INTO exclusions(id, type, expression) VALUES(NULL, 'glob', ?)", (args.glob[0],))

        if args.simple:
            c.execute("INSERT INTO exclusions(id, type, expression) VALUES(NULL, 'simple', ?)", (args.simple[0],))

        if args.rm or args.re or args.glob or args.simple:
            logger.warning("You may need to resync")

    # TODO: we can tell them that they need to resync, but we should also
    # automatically remove DB items that they've just now excluded (so that they
    # only have to resync on *removing* an exclusion)

if __name__ == '__main__':
    main()
