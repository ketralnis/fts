#!/usr/bin/env python

from ftssync import sync
from ftsdb import createdb
from ftsdb import logger

def init(cwd, initsync=True):
    dbfname, conn = createdb(cwd)
    logger.info("Created %s", dbfname)

    if initsync:
        with conn:
            # add the initial documents
            sync(conn, cwd, '')

    return dbfname
