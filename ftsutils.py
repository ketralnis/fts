from contextlib import contextmanager
import time
from datetime import datetime

@contextmanager
def timeit(label):
    now = datetime.now()
    print 'Starting %s at %s' % (label, now.strftime('%Y-%m-%d %H:%M:%S'))
    start = time.time()

    yield

    now = datetime.now()
    end = time.time()
    took = time.time() - start
    print '%s finished at %s in %.2fs' % (label, now.strftime('%Y-%m-%d %H:%M:%S'), took)

