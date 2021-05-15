class Field(str):

    def __new__(cls, content, *, header):
        return super().__new__(cls, content)

    def __init__(self, content, *, header):
        self.header = header

    @property
    def bool(self):
        if self.lower() in ('true', 't', 'y', 'yes', '1', 'on'):
            return True
        elif self.lower() in ('false', 'f', 'n', 'no', '0', 'off'):
            return False
        else:
            raise ValueError(f'Cannot convert "{self}" to bool')

    @property
    def int(self):
        return int(self)

    @property
    def float(self):
        return float(self)

    @property
    def str(self):
        return str(self)

    @property
    def bytes(self):
        return bytes(self)

    def _isnumber(self):
        try:
            float(self)
            return True
        except ValueError:
            return False

    def _coerce_to_number(self):
        try:
            try:
                return int(self)
            except ValueError:
                return float(self)
        except ValueError:
            raise ValueError(f'Cannot convert "{self}" to int or float')

    def _coerce_with_type_check(self, other):
        '''
        Perform numeric type coercion via type check. If we can coerce to the
        "other" type, perform the coercion. Otherwise use the super
        implementation.
        '''
        if isinstance(other, Field) and self._isnumber() and other._isnumber():
            return self._coerce_to_number(), other._coerce_to_number()
        elif isinstance(other, (int, float)):
            return self._coerce_to_number(), other
        else:
            return super(), other

    def _coerce_with_fallback(self, other):
        '''
        Perform numeric type coercion by converting self to a numeric value
        (without checking the `other` type). Falls back to the super
        implementation if the coercion failed.
        '''
        try:
            return self._coerce_assuming_numeric(other)
        except ValueError:
            return super(), other

    def _coerce_assuming_numeric(self, other):
        '''
        Perform numeric type coercion assuming both self and other can be
        successfully converted to numeric types. An exception will be thrown
        if the coercion failed.
        '''
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
        return modified_self.__radd__(modified_other)

    def __sub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__sub__(modified_other)

    def __rsub__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rsub__(modified_other)

    def __mul__(self, other):
        '''
        Multiplication is one special case where the numeric operation takes
        precendence over the string operation, since the former is more common.
        '''
        modified_self, modified_other = self._coerce_with_fallback(other)
        return modified_self.__mul__(modified_other)

    def __matmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__matmul__(modified_other)

    def __truediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__truediv__(modified_other)

    def __floordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__floordiv__(modified_other)

    def __mod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__mod__(modified_other)

    def __divmod__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__divmod__(modified_other)

    def __pow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__pow__(modified_other, *args)

    def __lshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__lshift__(modified_other)

    def __rshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rshift__(modified_other)

    def __and__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__and__(modified_other)

    def __xor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__xor__(modified_other)

    def __or__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__or__(modified_other)

    def __rmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rmul__(modified_other)

    def __rmatmul__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rmatmul__(modified_other)

    def __rtruediv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rtruediv__(modified_other)

    def __rfloordiv__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rfloordiv__(modified_other)

    def __rpow__(self, other, *args):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rpow__(modified_other, *args)

    def __rlshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rlshift__(modified_other)

    def __rrshift__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rrshift__(modified_other)

    def __rand__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rand__(modified_other)

    def __rxor__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__rxor__(modified_other)

    def __ror__(self, other):
        modified_self, modified_other = self._coerce_assuming_numeric(other)
        return modified_self.__ror__(modified_other)

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
        return self.encode('utf-8')
