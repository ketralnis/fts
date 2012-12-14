import os
import os.path
import errno
import stat
import time
from functools import partial
import logging

from ftsdb import update_document, add_document, prefix_expr, logger, Cursor

def visitor(path, prefix, simple_exclusions, cu, dirname, fnames):
    if simple_exclusions:
        simple_removes = set(fnames).intersection(simple_exclusions)
        for sr in simple_removes:
            fnames.remove(sr)

    if logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Walking %s", dirname)
        fnames.sort() # makes the child 'walking' messages come out in an order the user expects

    for sname in fnames:
        fname = os.path.join(dirname, sname)

        assert fname.startswith(path)
        if prefix:
            assert fname.startswith(os.path.join(path, prefix))

        dbfname = fname[len(path)+1:]

        try:
            st = os.stat(fname)
            mode = st.st_mode
            if stat.S_ISDIR(mode):
                continue
            if not stat.S_ISREG(mode):
                logger.warn("Skipping non-regular file %s (%s)", dbfname, stat.S_IFMT(mode))
                continue
        except IOError as e:
            if e.errno == errno.ENOENT:
                # it was deleted in between
                continue
            raise

        cu.execute("INSERT INTO ondisk(path, dbpath, last_modified) VALUES (?, ?, ?)",
                   (fname, dbfname, int(st[stat.ST_MTIME])))

def sync(conn, path, prefix, files = None):
    # path must be a full path on disk
    # prefix must be the full path on disk that we're syncing (or empty)

    start = time.time()

    news = updates = deletes = 0

    with Cursor(conn) as c, Cursor(conn) as cu:
        c.execute("""
                  CREATE TEMPORARY TABLE
                  ondisk (
                     path          TEXT PRIMARY KEY COLLATE BINARY,
                     dbpath        TEXT COLLATE BINARY,
                     last_modified INTEGER
                  );
                  """)

        c.execute("SELECT expression FROM exclusions WHERE type = 'simple'")
        simple_exclusions = set(x[0] for x in c.fetchall())

        wpath = path
        if prefix:
            wpath = os.path.join(path, prefix)

        if files is None:
            os.path.walk(wpath, partial(visitor, path, prefix, simple_exclusions), cu)
        else:
            visitor(path, prefix, simple_exclusions, cu, wpath, files)

        # remove anyone that matches our ignore globs
        cu.execute("""
            DELETE FROM ondisk WHERE path in
            (SELECT od.path
               FROM ondisk od, exclusions e
              WHERE (e.type = 'glob'   AND od.path GLOB   e.expression)
                 OR (e.type = 're'     AND od.path REGEXP e.expression)
                 OR (e.type = 'simple' AND IGNORE_SIMPLE(od.path, e.expression)
            )
        )""")
        ignores = cu.rowcount

        # now build three groups: new files to be added, missing files to be
        # deleted, and old files to be updated

        # updated ones
        cu.execute("""
            CREATE TEMPORARY TABLE updated_files AS
            SELECT f.docid AS docid,
                   od.path AS path,
                   od.last_modified AS last_modified
              FROM ondisk od, files f
             WHERE od.dbpath = f.path
               AND f.last_modified < od.last_modified
        """)

        # new files to create
        cu.execute("""
            CREATE TEMPORARY TABLE created_files AS
            SELECT od.path AS path,
                   od.dbpath AS dbpath,
                   od.last_modified
              FROM ondisk od
             WHERE od.dbpath NOT IN (SELECT path FROM files)
        """)

        # files that we've indexed in the past but don't exist anymore
        cu.execute("""
            CREATE TEMPORARY TABLE deletedocs AS
            SELECT f.docid AS docid
              FROM files f
             WHERE (? = '' OR f.path LIKE ? ESCAPE '\\')
               AND f.path NOT IN (SELECT dbpath FROM ondisk od);
        """, (prefix, prefix_expr(prefix),))

        # set up our debugging progress-printing closure
        def printprogress(*a):
            pass
        if logger.getEffectiveLevel() <= logging.DEBUG:
            progresstotal = (cu.execute("SELECT COUNT(*) FROM updated_files").fetchone()[0] +
                             cu.execute("SELECT COUNT(*) FROM created_files").fetchone()[0])
            if progresstotal > 0:
                def printprogress(s, updates, news, fname):
                    logger.debug("%d/%d (%.1f%%) %s: %s", updates+news, progresstotal, float(updates+news)/progresstotal*100, s, fname)

        c.execute("SELECT docid, path, last_modified FROM updated_files;")
        for (docid, path, last_modified) in c:
            printprogress("Updating", updates, news, path)
            try:
                update_document(cu, docid, last_modified, open(path).read())
            except IOError as e:
                if e.errno in (errno.ENOENT, errno.EPERM):
                    logger.warning("Skipping %s: %s", path, os.strerror(e.errno))
                else:
                    raise
                continue
            updates += 1

        if files is None:
            # don't delete non-matching files if we've been given a specific
            # list of files to update
            c.execute("""
                DELETE FROM files WHERE docid IN (SELECT docid FROM deletedocs);
            """)
            del1 = c.rowcount
            c.execute("""
                DELETE FROM files_fts WHERE docid IN (SELECT docid FROM deletedocs);
            """)
            del2 = c.rowcount
            deletes = max(del1, del2)

        # new files to create
        c.execute("SELECT path, dbpath, last_modified FROM created_files;")
        for (fname, dbpath, last_modified) in c:
            # is it safe to re-use the last_modified that we got before, or do
            # we need to re-stat() the file? reusing it like this could make a
            # race-condition whereby we never re-update that file
            printprogress("Adding", updates, news, fname)
            try:
                add_document(cu, dbpath, last_modified, open(fname).read())
            except IOError as e:
                if e.errno in (errno.ENOENT, errno.EPERM):
                    logger.warning("Skipping %s: %s", fname, os.strerror(e.errno))
                else:
                    raise
                continue
            news += 1

        logger.info("%d new documents, %d deletes, %d updates, %d ignored in %.2fs", news, deletes, updates, ignores, time.time()-start)

        cu.execute("DROP TABLE updated_files;")
        cu.execute("DROP TABLE created_files;")
        cu.execute("DROP TABLE deletedocs;")
        cu.execute("DROP TABLE ondisk;")
