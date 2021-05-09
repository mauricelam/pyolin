import collections.abc
import functools
import itertools
import os
import sys


def cache(func):
    return functools.lru_cache(maxsize=None)(func)


def cached_property(func):
    return property(cache(func))


def debug(*args):
    if os.getenv('DEBUG'):
        print(*args, file=sys.stderr)


_UNDEFINED_ = object()


class LazySequence(collections.abc.Sequence):
    '''
    An iterator that also implements the sequence interface
    '''
    def __init__(self, iterator):
        self._iter = iterator
        self._list = None
        self._itered = False

    @property
    def list(self):
        if not self._list:
            self._list = list(iter(self))
        return self._list

    def __iter__(self):
        if self._list:
            return iter(self._list)
        # Tee the iterator so every call to iter starts from the beginning
        result, self._iter = itertools.tee(self._iter, 2)
        return result

    def __getitem__(self, key):
        if self._list:
            return self._list.__getitem__(key)
        if isinstance(key, slice):
            return itertools.islice(iter(self), key.start, key.stop, key.step)
        try:
            return next(itertools.islice(iter(self), key, key + 1))
        except StopIteration:
            raise IndexError('list index out of range')

    def __reversed__(self):
        # Not necessary, but is probably (slightly) faster than the default
        # implementation that uses __getitem__ and __len__
        return reversed(self.list)

    def __len__(self):
        return len(self.list)

    def __add__(self, other):
        return LazySequence(itertools.chain(self, other))

    def __radd__(self, other):
        return LazySequence(itertools.chain(other, self))


class LazyItemDict(dict):
    '''
    A dict that can evaluate LazyItems on demand when they are accessed.
    '''
    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if isinstance(value, LazyItem):
            return value()
        return value


class LazyItem:
    '''
    Item for ImplicitVarsDict that is evaluated on demand.
    '''
    def __init__(self, func, *, cache=True, on_accessed=None):
        self.func = func
        self._val = None
        self._cache_state = 'no_cached_value' if cache else 'dont_cache'
        self._on_accessed = on_accessed

    def __call__(self, *arg, **kwargs):
        if self._cache_state == 'cached':
            return self._val
        value = self.func(*arg, **kwargs)
        if self._on_accessed:
            self._on_accessed()
        if self._cache_state != 'dont_cache':
            self._val = value
            self._cache_state = 'cached'
        return value
