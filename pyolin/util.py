import abc
import collections.abc
import functools
import itertools
import os
import sys
import importlib
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)


def cache(func):
    return functools.lru_cache(maxsize=None)(func)


def cached_property(func):
    return property(cache(func))


def debug(*args: Any) -> None:
    """
    Print a debug statement. These are printed to the console if the $DEBUG env var is set
    """
    if os.getenv("DEBUG"):
        print(*args, file=sys.stderr)


class Undefined:
    def __str__(self):
        return ""

    def __repr__(self):
        return "Undefined()"

    def __bool__(self):
        return False

    def __bytes__(self):
        return b""


_UNDEFINED_ = Undefined()


class NoMoreRecords(StopIteration):
    pass


T = TypeVar("T")


class StreamingSequence(Sequence[T]):
    """
    An iterator that also implements the sequence interface. This is "streaming" in the sense that
    it will try its best give the results as soon as we can get the answer from the available input,
    instead of eagerly looking through all of the items in the given iterator.
    """

    def __init__(self, iterator):
        self._iter = iterator
        self._list = None

    @property
    def list(self) -> List[T]:
        if not self._list:
            self._list = list(iter(self))
        return self._list

    def __iter__(self) -> Iterator:
        if self._list:
            return iter(self._list)
        # Tee the iterator so every call to iter starts from the beginning
        result, self._iter = itertools.tee(self._iter, 2)
        return result

    def __getitem__(self, key: Union[slice, int]) -> Union[Iterable[T], T]:
        if self._list:
            return self._list.__getitem__(key)
        if isinstance(key, slice):
            if (
                (key.start is not None and key.start < 0)
                or (key.stop is not None and key.stop < 0)
                or (key.step is not None and key.step < 0)
            ):
                # Iterators can't do negative indexing. Materialize to a list
                return self.list[key]
            return itertools.islice(iter(self), key.start, key.stop, key.step)
        if key < 0:
            # Iterators can't do negative indexing. Materialize to a list
            return self.list[key]
        try:
            return next(itertools.islice(iter(self), key, key + 1))
        except StopIteration:
            # pylint:disable=raise-missing-from
            raise IndexError("list index out of range")

    def __reversed__(self) -> Iterable[T]:
        # Not necessary, but is probably (slightly) faster than the default
        # implementation that uses __getitem__ and __len__
        return reversed(self.list)

    def __len__(self) -> int:
        return len(self.list)

    def __add__(self, other: Iterable[T]) -> Iterable[T]:
        return StreamingSequence(itertools.chain(self, other))

    def __radd__(self, other: Iterable[T]) -> Iterable[T]:
        return StreamingSequence(itertools.chain(other, self))

    def __str__(self) -> str:
        return str(self.list)

    def __repr__(self) -> str:
        return repr(self.list)


class ItemDict(dict):
    """
    A dict that can evaluate LazyItems on demand when they are accessed.
    """

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
        debug(f"dict setting {key}={value}")
        return super().__setitem__(key, value)

    def __missing__(self, key):
        try:
            return importlib.import_module(key)
        except ModuleNotFoundError:
            raise KeyError(key)


I = TypeVar("I")


class Item(Generic[I]):
    """An Item that is defined by a function, which will be initialized when this item is used,
    typically from ItemDict."""

    def __init__(self, func: Callable[..., I]):
        self.func = func

    def __call__(self, *arg, **kwargs) -> I:
        return self.func(*arg, **kwargs)


class SettableItem(Item[I], metaclass=abc.ABCMeta):
    """A settable item for use inside an ItemDict."""

    @abc.abstractmethod
    def set(self, value: I) -> None:
        """Method to be called when the value of this item is set"""
        raise NotImplementedError()


class BoxedItem(SettableItem[I]):
    """A SettableItem that just stores its value in a field, and can be frozen."""

    def __init__(self, func: Callable[..., I]):
        super().__init__(func)
        self.value = None
        self.frozen = False

    def set(self, value: I) -> None:
        debug("setting boxed value ", value)
        if self.frozen:
            raise RuntimeError("Cannot set parser after it has been used")
        self.value = value

    def __call__(self) -> I:
        if self.value is not None:
            return self.value
        self.value = super().__call__()
        return self.value


class LazyItem(Item[I]):
    """
    Item for ItemDict that is evaluated on demand.
    """

    def __init__(
        self,
        func: Callable[..., I],
        *,
        on_accessed: Optional[Callable[[], None]] = None,
    ):
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


def peek_iter(iterator: Iterable[T], num: int) -> Tuple[Sequence[T], Iterable[T]]:
    if isinstance(iterator, collections.abc.Sequence):
        return iterator[:num], iterator
    iterator = iter(iterator)  # Ensure this is an iterator
    preview = tuple(itertools.islice(iterator, 0, num))
    return preview, itertools.chain(preview, iterator)


def peek_one_iter(iterator: Iterable[T], default: T) -> Tuple[T, Iterable[T]]:
    peek, iterator = peek_iter(iterator, 1)
    return next(iter(peek), default), iterator


def tee_if_iterable(obj: Any) -> Tuple[Any, Any]:
    if isinstance(obj, collections.abc.Iterable):
        if not isinstance(obj, (collections.abc.Sequence, dict)):
            pd = sys.modules.get("pandas", None)
            if not (pd and isinstance(obj, pd.DataFrame)):
                return itertools.tee(obj)
    return obj, obj


def is_list_like(obj: Any) -> bool:
    if not isinstance(obj, collections.abc.Iterable):
        return False
    if isinstance(obj, (str, bytes, dict)):
        return False
    return True


# https://bugs.python.org/issue11380#msg248579
def clean_close_stdout_and_stderr() -> None:
    try:
        sys.stdout.flush()
    finally:
        try:
            sys.stdout.close()
        finally:
            try:
                sys.stderr.flush()
            finally:
                sys.stderr.close()
