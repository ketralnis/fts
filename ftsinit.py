#!/usr/bin/env python

import os

from ftssync import sync
from ftsdb import createdb
from ftsdb import logger

def init(cwd, initsync=True):
    dbfname, conn = createdb(cwd)
    logger.info("Created %s", dbfname)

    if initsync:
        # add the initial documents
        sync(conn, cwd, '')

if __name__ == '__main__':
    main()