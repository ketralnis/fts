import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

from db import conn
from db import get_or_createdb
from ftssync import sync

def main():
    dbid = get_or_createdb(conn.cursor(), "mydb")
    sync(dbid, "/Users/dking/src/fts/rando")

if __name__ == '__main__':
    main()