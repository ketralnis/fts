import os.path
import stat
import logging

from db import update_document, add_document
from db import conn

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def sync(dbid, path):
    with conn: # transaction
        news = updates = deletes = 0
        c = conn.cursor() # the cursor we use for reading
        cu = conn.cursor() # the cursor we use for updating

        try:
            c.execute("""
                      CREATE TEMPORARY TABLE
                      ondisk (
                         path TEXT PRIMARY KEY,
                         last_modified INTEGER
                      );
                      """)

            def visitor(arg, dirname, fnames):
                assert arg is None

                for sname in fnames:
                    fname = os.path.join(dirname, sname)
                    st = os.stat(fname)
                    mode = st.st_mode
                    if not stat.S_ISREG(mode):
                        logging.warn("Skipping non-regular file %s (%s)", fname, mode | stat.S_IFMT)
                        continue
                    cu.execute("INSERT INTO ondisk(path, last_modified) VALUES (?, ?)",
                              (fname, int(st[stat.ST_MTIME])))

            os.path.walk(path, visitor, None)

            # now build three groups: new files to be added, missing files to be
            # deleted, and old files to be updated

            # updated ones
            c.execute("""
                SELECT f.docid, f.path, od.last_modified
                  FROM ondisk od, files f
                 WHERE f.dbid = ?
                   AND od.path = f.path
                   AND f.last_modified < od.last_modified
            """, (dbid,))
            for (docid, fname, last_modified) in c:
                update_document(cu, docid, last_modified, open(fname).read())
                updates += 1

            # ones to delete
            c.execute("""
                      CREATE TEMPORARY TABLE deletedocs AS
                      SELECT f.docid
                        FROM files f
                       WHERE f.path NOT IN (SELECT path FROM ondisk od)
                         AND f.dbid = ?
            """, (dbid,))
            c.execute("""
                DELETE FROM files WHERE docid IN (SELECT docid FROM deletedocs);
            """)
            c.execute("""
                DELETE FROM files_fts WHERE docid IN (SELECT docid FROM deletedocs);
            """)
            deletes = c.execute("SELECT COUNT(*) FROM deletedocs").fetchone()[0]
            c.execute("""
                DROP TABLE deletedocs;
            """)

            # new files to create
            c.execute("""
                 SELECT od.path, od.last_modified
                   FROM ondisk od
                  WHERE od.path NOT IN (SELECT path FROM files f WHERE f.dbid = ?)
            """, (dbid,))

            for (fname, last_modified) in c:
                # TODO: is it safe to re-use the last_modified that we got before,
                # or do we need to re-stat() the file?
                add_document(cu, dbid, fname, last_modified, open(fname).read())
                news += 1

            return (news, deletes, updates)

        finally:
            c.close()
            cu.close()
