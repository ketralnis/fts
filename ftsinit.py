#!/usr/bin/env python

import os

from ftssync import sync
from db import createdb, logger

def main():
    cwd = os.getcwd()
    dbfname, conn = createdb(cwd)
    logger.info("Created %s", dbfname)

    # TODO: verbose sync

    # add the initial documents
    news, deletes, updates = sync(conn, cwd, '')
    logger.info("Added %d documents (%d/%d)", news, deletes, updates)

if __name__ == '__main__':
    main()