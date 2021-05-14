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


class NoMoreRecords(StopIteration):
    pass


class StreamingSequence(collections.abc.Sequence):
    '''
    An iterator that also implements the sequence interface. This is "streaming" in the sense that
    it will try its best give the results as soon as we can get the answer from the available input,
    instead of eagerly looking through all of the items in the given iterator.
    '''
    def __init__(self, iterator):
        self._iter = iterator
        self._list = None

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
        return StreamingSequence(itertools.chain(self, other))

    def __radd__(self, other):
        return StreamingSequence(itertools.chain(other, self))


class ItemDict(dict):
    '''
    A dict that can evaluate LazyItems on demand when they are accessed.
    '''
    def __getitem__(self, key):
        value = dict.__getitem__(self, key)
        if isinstance(value, Item):
            return value()
        return value

    def __setitem__(self, key, value):
        try:
            oldval = super().__getitem__(key)
            if isinstance(oldval, SettableItem):
                oldval.set(value)
                return
        except KeyError:
            pass
        debug(f'dict setting {key}={value}')
        return super().__setitem__(key, value)


class Item:
    def __init__(self, func):
        self.func = func

    def __call__(self, *arg, **kwargs):
        return self.func(*arg, **kwargs)


class SettableItem(Item):
    def set(self, value):
        raise NotImplemented()


class BoxedItem(SettableItem):
    def __init__(self, func):
        super().__init__(func)
        self.value = None
        self.frozen = False

    def set(self, value):
        debug('setting boxed value ', value)
        if self.frozen:
            raise RuntimeError('Cannot set parser after it has been used')
        self.value = value

    def __call__(self):
        if self.value is not None:
            return self.value
        self.value = super().__call__()
        return self.value


class LazyItem(Item):
    '''
    Item for ItemDict that is evaluated on demand.
    '''
    def __init__(self, func, *, on_accessed=None):
        super().__init__(func)
        self._val = None
        self._cached = False
        self._on_accessed = on_accessed

    def __call__(self, *arg, **kwargs):
        if not self._cached:
            self._val = super().__call__(*arg, **kwargs)
            if self._on_accessed:
                self._on_accessed()
            self._cached = True
        return self._val
