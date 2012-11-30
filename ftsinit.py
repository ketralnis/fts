import os.path
import sys

from ftsdb import createdb
from ftsdb import logger, _db_name

def init(cwd, compress=False):
    if os.path.isfile(os.path.join(cwd, _db_name)):
        logger.error("Cowardly refusing to overwrite existing %s", _db_name)
        sys.exit(1)

    dbfname, conn = createdb(cwd, compress=compress)
    logger.info("Created %s", dbfname)
    return dbfname
