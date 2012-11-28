import sys
import os
import os.path
import stat
import logging

from db import update_document, add_document, _db_name, finddb, prefix_expr

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ignores = set(['.git', _db_name])

def sync(conn, path, prefix):
    # path must be a full path on disk
    # prefix must be the full path on disk that we're syncing (or empty)

    with conn: # transaction
        news = updates = deletes = 0
        c = conn.cursor() # the cursor we use for reading
        cu = conn.cursor() # the cursor we use for updating

        try:
            c.execute("""
                      CREATE TEMPORARY TABLE
                      ondisk (
                         path          TEXT PRIMARY KEY,
                         dbpath        TEXT,
                         last_modified INTEGER
                      );
                      """)

            def visitor(arg, dirname, fnames):
                assert arg is None

                removals = []
                for fname in fnames:
                    if fname in ignores:
                        removals.append(fname)
                for r in removals:
                    fnames.remove(r)

                for sname in fnames:
                    if sname in ignores:
                        continue

                    fname = os.path.join(dirname, sname)

                    assert fname.startswith(path)
                    if prefix:
                        assert fname.startswith(os.path.join(path, prefix))

                    dbfname = fname[len(path)+1:]

                    st = os.stat(fname)
                    mode = st.st_mode
                    if stat.S_ISDIR(mode):
                        continue
                    if not stat.S_ISREG(mode):
                        logging.warn("Skipping non-regular file %s (%s)", dbfname, stat.S_IFMT(mode))
                        continue
                    cu.execute("INSERT INTO ondisk(path, dbpath, last_modified) VALUES (?, ?, ?)",
                               (fname, dbfname, int(st[stat.ST_MTIME])))

            wpath = path
            if prefix:
                wpath = os.path.join(path, prefix)
            os.path.walk(wpath, visitor, None)

            # now build three groups: new files to be added, missing files to be
            # deleted, and old files to be updated

            # updated ones
            c.execute("""
                SELECT f.docid, od.path, od.last_modified
                  FROM ondisk od, files f
                 WHERE od.dbpath = f.path
                   AND f.last_modified < od.last_modified
            """)
            for (docid, path, last_modified) in c:
                update_document(cu, docid, last_modified, open(path).read())
                updates += 1

            # ones to delete
            c.execute("""
                      CREATE TEMPORARY TABLE deletedocs (
                          docid integer primary KEY
                      );
            """)
            c.execute("""
                      INSERT INTO deletedocs(docid)
                      SELECT f.docid
                        FROM files f
                       WHERE (? = '' OR f.path LIKE ? ESCAPE '\\')
                         AND f.path NOT IN (SELECT dbpath FROM ondisk od)
            """, (prefix, prefix_expr(prefix),))
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
                 SELECT od.path, od.dbpath, od.last_modified
                   FROM ondisk od
                  WHERE od.dbpath NOT IN (SELECT path FROM files)
            """)
            for (fname, dbpath, last_modified) in c:
                # TODO: is it safe to re-use the last_modified that we got before,
                # or do we need to re-stat() the file?
                add_document(cu, dbpath, last_modified, open(fname).read())
                news += 1

            return (news, deletes, updates)

        finally:
            c.close()
            cu.close()

def main():
    # TODO: verbose sync
    root, prefix, conn = finddb(os.getcwd())
    sync(conn, root, prefix)

if __name__ == '__main__':
    main()
