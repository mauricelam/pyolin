from typing import Any, Optional, Tuple, Union


Number = Union[int, float]
Boolean = bool


class DeferredType(str):
    """A string that defers typing itself to wait for more information based on
    the operations performed on it. This type will try to coerce itself into
    numeric types when multiplication is requested, for example.

    In order to explicitly type this, use `.int`, `.bool`, `.str`, or
    `.bytes`."""

    def __new__(cls, content: Union['DeferredType', str, bytes]):
        if isinstance(content, DeferredType):
            return content
        elif isinstance(content, (bytes, bytearray)):
            return super().__new__(cls, content.decode("utf-8", errors="replace"))
        return super().__new__(cls, content)

    def __init__(self, content: Union['DeferredType', str, bytes]):
        if isinstance(content, DeferredType):
            self.source = content.source
            self.is_valid_str = content.is_valid_str
        elif isinstance(content, str):
            self.source = content
            self.is_valid_str = True
        else:
            self.source = content
            try:
                content.decode("utf-8")
                self.is_valid_str = True
            except UnicodeDecodeError:
                self.is_valid_str = False

    def _isnumber(self):
        try:
            float(self)
            return True
        except ValueError:
            return False

    def _coerce_to_number(self) -> Number:
        try:
            try:
                return int(self)
            except ValueError:
                return float(self)
        except ValueError:
            # pylint:disable=raise-missing-from
            raise ValueError(f'Cannot convert "{self}" to int or float')

    def _coerce_with_type_check(self, other: Any) -> Tuple[Union[Number, str], Any]:
        """
        Perform numeric type coercion via type check. If we can coerce to the
        "other" type, perform the coercion. Otherwise use the super
        implementation.
        """
        if isinstance(other, Field) and self._isnumber() and other._isnumber():
            return self._coerce_to_number(), other._coerce_to_number()
        elif isinstance(other, (int, float)):
            return self._coerce_to_number(), other
        else:
            return super(), other

    def _coerce_with_fallback(self, other: Any) -> Tuple[Union[Number, str], Any]:
        """
        Perform numeric type coercion by converting self to a numeric value
        (without checking the `other` type). Falls back to the super
        implementation if the coercion failed.
        """
        try:
            return self._coerce_assuming_numeric(other)
        except ValueError:
            return self.str, other

    def _coerce_assuming_numeric(self, other):
        """
        Perform numeric type coercion assuming both self and other can be
        successfully converted to numeric types. An exception will be thrown
        if the coercion failed.
        """
        if isinstance(other, Field):
            other = other._coerce_to_number()
        return self._coerce_to_number(), other

    def __gt__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__gt__(modified_other)

    def __ge__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__ge__(modified_other)

    def __lt__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__lt__(modified_other)

    def __le__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__le__(modified_other)

    def __eq__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__eq__(modified_other)

    def __add__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__add__(modified_other)

    def __radd__(self, other):
        modified_self, modified_other = self._coerce_with_type_check(other)
        return modified_self.__radd__(modified_other)  # type: ignore

    def __sub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__sub__(modified_other)  # type: ignore

    def __rsub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rsub__(modified_other)  # type: ignore

    def __mul__(self, other):
        """
        Multiplication is one special case where the numeric operation takes
        precendence over the string operation, since the former is more common.
        """
        modified_self, modified_other = self._coerce_with_fallback(other)
        return modified_self.__mul__(modified_other)

    def __matmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        # pylint: disable=no-member
        return modified_self.__matmul__(modified_other)  # type: ignore
        # pylint: enable=no-member

    def __truediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__truediv__(modified_other)  # type: ignore

    def __floordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__floordiv__(modified_other)  # type: ignore

    def __mod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__mod__(modified_other)  # type: ignore

    def __divmod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__divmod__(modified_other)  # type: ignore

    def __pow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__pow__(modified_other, *args)  # type: ignore

    def __lshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__lshift__(modified_other)  # type: ignore

    def __rshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rshift__(modified_other)  # type: ignore

    def __and__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__and__(modified_other)  # type: ignore

    def __xor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__xor__(modified_other)  # type: ignore

    def __or__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__or__(modified_other)  # type: ignore

    def __rmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rmul__(modified_other)  # type: ignore

    def __rmatmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        # pylint: disable=no-member
        return modified_self.__rmatmul__(modified_other)  # type: ignore
        # pylint: enable=no-member

    def __rtruediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rtruediv__(modified_other)  # type: ignore

    def __rfloordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rfloordiv__(modified_other)  # type: ignore

    def __rpow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rpow__(modified_other, *args)  # type: ignore

    def __rlshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rlshift__(modified_other)  # type: ignore

    def __rrshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rrshift__(modified_other)  # type: ignore

    def __rand__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rand__(modified_other)  # type: ignore

    def __rxor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rxor__(modified_other)  # type: ignore

    def __ror__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__ror__(modified_other)  # type: ignore

    def __neg__(self):
        return self._coerce_to_number().__neg__()

    def __pos__(self):
        return self._coerce_to_number()

    def __abs__(self):
        return self._coerce_to_number().__abs__()

    def __round__(self, *args):
        return self._coerce_to_number().__round__(*args)

    def __floor__(self):
        return self._coerce_to_number().__floor__()

    def __ceil__(self):
        return self._coerce_to_number().__ceil__()

    def __bytes__(self):
        return self.bytes

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        return self.bool

    def __len__(self):
        if not self.is_valid_str:
            raise TypeError('Cannot get length of str containing non-UTF8')
        return super().__len__()

    @property
    def bool(self) -> bool:
        """Converts this deferred type to a boolean."""
        if self.lower() in ("true", "t", "y", "yes", "1", "on"):
            return True
        elif self.lower() in ("false", "f", "n", "no", "0", "off"):
            return False
        else:
            raise ValueError(f'Cannot convert "{self}" to bool')

    @property
    def int(self) -> int:
        """Converts this deferred type to an int."""
        return int(self)

    @property
    def float(self) -> float:
        """Converts this deferred type to a float."""
        return float(self)

    @property
    def str(self) -> str:
        """Convers this deferred type to a string."""
        return str(self)

    @property
    def bytes(self) -> bytes:
        """Converts the data to bytes"""
        if isinstance(self.source, bytes):
            return self.source
        return super().encode("utf-8")


class Field(DeferredType):
    """Represents a field in a parsed input data table."""

    def __new__(cls, content, *, header: Optional["Field"]):
        return super().__new__(cls, content)

    def __init__(self, content, *, header: Optional["Field"]):
        super().__init__(content)
        self.header = header
