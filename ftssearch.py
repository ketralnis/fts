import os
import stat
import itertools
from collections import namedtuple

from ftsdb import prefix_expr
from ftsdb import logger, Cursor

snippet_color        = '\x1b[01;33m'
snippet_end_color    = '\x1b[00m'
snippet_elipsis      = ''.join([snippet_color, '...', snippet_end_color])

filename_color       = '\x1b[01;31m'
filename_end_color   = '\x1b[00m'

def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # from http://docs.python.org/2/library/itertools.html#recipes
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

SearchOffset = namedtuple('SearchOffset', ('offset', 'length'))

class SearchResult(object):
    __slots__ = ('filename', 'offsets', 'snippet')

    def __init__(self, filename, offsets, snippet):
        self.filename = filename
        self.offsets = self.parse_offsets(offsets)
        self.snippet = snippet

    def parse_offsets(self, offsets):
        ret = []

        nums = map(int, offsets.split())
        assert len(nums) % 4 == 0
        for colno, termno, offset, length in grouper(4, nums):
            ret.append(SearchOffset(offset, length))

        return ret

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__,
                                   self.filename,
                                   self.offsets,
                                   self.snippet)

    def colorize(self, s, color=True):
        if color:
            return ''.join((filename_color, s, filename_end_color))
        else:
            return s

    def format(self, color=False):
        if self.snippet:
                return self.colorize(self.filename, color) + ':\n' + '\n'.join('\t' + x for x in self.snippet.split('\n'))
        else:
            return self.colorize(self.filename, color)

def search(conn, prefix, term, mode, checksync = True, color=False):
    assert mode in ('MATCH', 'REGEXP')

    with Cursor(conn) as c:
        prefix = prefix or ''
        prefixexpr = prefix_expr(prefix)
        needsync = 0
        c.execute("""
            SELECT f.path, f.last_modified,
                   offsets(ft.files_fts),
                   snippet(ft.files_fts, ?, ?, ?, -1, -10)
              FROM files f, files_fts ft
             WHERE f.docid = ft.docid
               AND (? = '' OR f.path LIKE ? ESCAPE '\\') -- use the prefix if present -- ESCAPE disables the LIKE optimization :(
               AND ft.body %(mode)s ?
          -- TODO: this runs simple_rank, which calls a Python function, many
          -- times per row. we can decompose this to a subselect to avoid this
          ORDER BY simple_rank(matchinfo(ft.files_fts))
        """ % dict(mode=mode), (snippet_color if color else '',
                                snippet_end_color if color else '',
                                snippet_elipsis if color else '...',
                                prefix,
                                prefixexpr,
                                term,))
        for (path, last_modified, offsets, snippet) in c:

            if prefix:
                assert path.startswith(prefix)

            # if they're in a subdirectory, deprefix the filename
            shortpath = path[len(prefix)+1:] if prefix else path

            if checksync:
                # check if the returned files are known to be out of date. this
                # can be skipped when checksync is False (which means that a
                # sync was done before starting the search)
                try:
                    st = os.stat(shortpath)
                    if int(st[stat.ST_MTIME]) > last_modified:
                        needsync += 1
                except OSError:
                    needsync += 1

            yield SearchResult(shortpath, offsets, snippet)

        if needsync:
            logger.warning("%d files were missing or out-of-date, you may need to resync", needsync)

