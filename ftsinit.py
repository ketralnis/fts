#!/usr/bin/env python

from ftsdb import createdb
from ftsdb import logger

def init(cwd):
    dbfname, conn = createdb(cwd)
    logger.info("Created %s", dbfname)
    return dbfname
