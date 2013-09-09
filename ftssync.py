import os
import contextlib
import os.path
import errno
import stat
import time
import mmap
from functools import partial
import logging
import fnmatch

from ftsdb import re # re or re2

from ftsdb import update_document, add_document, remove_document
from ftsdb import prefix_expr, logger, Cursor

# nly index the first N bytes of a file
MAX_FSIZE = 1024*1024

@contextlib.contextmanager
def get_bytes(fname, size):
    """
    yield a python Buffer mapping to the first MAX_FSIZE bytes of the given file
    """
    size = min(size, MAX_FSIZE)

    if size == 0:
        yield ''
        return

    # try to save some memory by using the OS buffers instead of copying
    # the file contents
    with open(fname, 'rb') as f:
        with contextlib.closing(mmap.mmap(f.fileno(), size, access=mmap.ACCESS_READ)) as mm:
            yield buffer(mm, 0, size)

def should_allow(exclusions, basename, dbpath):
    """
    returns whether a given file should be allowed to exist based on our
    exclusion list
    """
    # exclusions =:= [{type, pattern}]

    for typ, pattern in exclusions:
        if typ == 'simple':
            if basename == pattern:
                return False

        elif typ == 'glob':
            # on basename only?
            if fnmatch.fnmatch(basename, pattern):
                return False

        elif typ == 're':
            if pattern.search(dbpath):
                # match or search? basename or full path or dbpath?
                return False

    return True

def visitor(path, prefix, exclusions, cu, dirname, fnames):
    if logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Walking %s", dirname)
        fnames.sort() # makes the child 'walking' messages come out in an order the user expects

    remove = []

    for basename in fnames:
        fname = os.path.join(dirname, basename)

        assert fname.startswith(path)
        if prefix:
            assert fname.startswith(os.path.join(path, prefix))

        dbfname = fname[len(path)+1:]

        if exclusions and not should_allow(exclusions, basename, dbfname):
            remove.append(basename)
            continue

        try:
            st = os.stat(fname)
            mode = st.st_mode
            size = st[stat.ST_SIZE]
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

        cu.execute("INSERT INTO ondisk(path, dbpath, last_modified, size) VALUES (?, ?, ?, ?)",
                   (fname, dbfname, int(st[stat.ST_MTIME]), size))

    if remove and logger.getEffectiveLevel() <= logging.DEBUG:
        logger.debug("Removing %r from walk", list(remove))
    for r in remove:
        fnames.remove(r)

def tcount(c, tname):
    return c.execute("SELECT COUNT(*) FROM %s;" % tname).fetchone()[0]

def sync(conn, path, prefix, files = None):
    # path must be a full path on disk
    # prefix must be the full path on disk that we're syncing (or empty)

    start = time.time()

    news = updates = deletes = 0
    tnews = tupdates = tdeletes = 0 # for debug printing

    with Cursor(conn) as c, Cursor(conn) as cu:
        c.execute("""
                  CREATE TEMPORARY TABLE
                  ondisk (
                     path          TEXT PRIMARY KEY COLLATE BINARY,
                     dbpath        TEXT COLLATE BINARY,
                     last_modified INTEGER,
                     size          INTEGER
                  );
                  """)

        exclusions = []
        c.execute("SELECT type AS typ, expression AS e FROM exclusions;")
        for typ, expression in c:
            if typ == 're':
                expression = re.compile(expression)
            exclusions.append((typ, expression))

        wpath = path
        if prefix:
            wpath = os.path.join(path, prefix)

        if files is None:
            os.path.walk(wpath, partial(visitor, path, prefix, exclusions), cu)
        else:
            visitor(path, prefix, exclusions, cu, wpath, files)

        logger.debug("Creating temporary index on ondisk(dbpath)")
        c.execute("CREATE INDEX tmp_ondisk_dbpath_idx ON ondisk(dbpath)")

        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug("Found %d files on disk", tcount(cu, "ondisk"))

        # now build three groups: new files to be added, missing files to be
        # deleted, and old files to be updated

        # updated ones
        cu.execute("""
            CREATE TEMPORARY VIEW updated_files AS
            SELECT f.docid AS docid,
                   od.path AS path,
                   od.last_modified AS last_modified,
                   od.size AS size
              FROM ondisk od, files f
             WHERE od.dbpath = f.path
               AND f.last_modified < od.last_modified
        """)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            tupdates = tcount(cu, "updated_files")
            logger.debug("Prepared %d files for updating", tupdates)

        # new files to create
        cu.execute("""
            CREATE TEMPORARY VIEW created_files AS
            SELECT od.path AS path,
                   od.dbpath AS dbpath,
                   od.last_modified,
                   od.size AS size
              FROM ondisk od
             WHERE NOT EXISTS(SELECT 1 FROM files f1 WHERE od.dbpath = f1.path)
        """)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            tnews = tcount(cu, "created_files")
            logger.debug("Prepared %d files for creation", tnews)

        # files that we've indexed in the past but don't exist anymore
        if files is None:
            # has to be a table instead of a view because parameters aren't allowed in views
            cu.execute("""
                CREATE TEMPORARY TABLE deletedocs AS
                SELECT f.docid AS docid,
                       f.path AS path
                  FROM files f
                 WHERE (? = '' OR f.path LIKE ? ESCAPE '\\') -- ESCAPE disables the LIKE optimization :(
                   AND NOT EXISTS(SELECT 1 FROM ondisk od WHERE od.dbpath = f.path)
            """, (prefix, prefix_expr(prefix)))
            if logger.getEffectiveLevel() <= logging.DEBUG:
                tdeletes = tcount(cu, "deletedocs")
                logger.debug("Prepared %d files for deletion", tdeletes)

        # set up our debugging progress-printing closure
        def printprogress(*a):
            pass
        if logger.getEffectiveLevel() <= logging.INFO:
            progresstotal = tnews + tupdates + tdeletes
            if progresstotal > 0:
                def printprogress(s, fname):
                    total = updates+news+deletes
                    percent = float(total)/progresstotal*100
                    logger.info("%d/%d (%.1f%%) %s: %s", total, progresstotal, percent, s, fname)

        # files that we've indexed in the past but don't exist anymore
        if files is None:
            c.execute("SELECT docid, path FROM deletedocs");
            for (docid, fname) in c:
                printprogress("Deleting", fname)
                remove_document(cu, docid)

                deletes += 1

        c.execute("SELECT docid, path, last_modified, size FROM updated_files;")
        for (docid, fname, last_modified, size) in c:
            printprogress("Updating %.2f" % (size/1024.0), fname)
            try:
                with get_bytes(fname, size) as bb:
                    update_document(cu, docid, last_modified, bb)
            except IOError as e:
                if e.errno in (errno.ENOENT, errno.EPERM):
                    logger.warning("Skipping %s: %s", fname, os.strerror(e.errno))
                else:
                    raise
                continue
            updates += 1

        # new files to create
        c.execute("SELECT path, dbpath, last_modified, size FROM created_files;")
        for (fname, dbpath, last_modified, size) in c:
            # is it safe to re-use the last_modified that we got before, or do
            # we need to re-stat() the file? reusing it like this could make a
            # race-condition whereby we never re-update that file
            printprogress("Adding %.1fk" % (size/1024.0), fname)
            try:
                with get_bytes(fname, size) as bb:
                    add_document(cu, dbpath, last_modified, bb)
            except IOError as e:
                if e.errno in (errno.ENOENT, errno.EPERM):
                    logger.warning("Skipping %s: %s", fname, os.strerror(e.errno))
                else:
                    raise
                continue
            news += 1

        logger.info("%d new documents, %d deletes, %d updates in %.2fs", news, deletes, updates, time.time()-start)

        cu.execute("DROP VIEW updated_files;")
        cu.execute("DROP VIEW created_files;")
        cu.execute("DROP TABLE IF EXISTS deletedocs;")
        cu.execute("DROP TABLE ondisk;")
