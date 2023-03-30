import sys
from dataclasses import dataclass

from descriptors import classonlymethod


class BStrTypeError(TypeError):

    __slots__ = ()

    def __init__(self, value, owner):
        super().__init__(value, owner)

    @property
    def value(self):
        return self.args[0]

    @property
    def owner(self):
        return self.args[1]

    @property
    def message(self):
        return f'argument must be bytes or str, not {self.value.__class__.__name__}'

    def __str__(self):
        return f'{self.owner} {self.message}'


_bs_dataclass = (
    dataclass(frozen=True,
              slots=True)
    if sys.version_info >= (3, 10) else
    dataclass(frozen=True)
)


@_bs_dataclass
class BytesStr:
    """Differentiate and compose str and bytes."""

    value_s: str
    value_b: bytes

    encoding = 'utf-8'

    BStrTypeError = BStrTypeError

    @classonlymethod
    def make(cls, value, /, encoding=encoding):
        """Extract bytes or str from given str or bytes value and return
        composition of two forms as BytesStr.

        """
        if isinstance(value, BytesStr):
            return value

        if isinstance(value, bytes):
            return cls(value.decode(encoding), value)

        if isinstance(value, str):
            return cls(value, value.encode(encoding))

        raise cls.BStrTypeError(value, owner=f'{cls.__name__}.make')

    def __str__(self):
        return self.value_s

    def __bytes__(self):
        return self.value_b
