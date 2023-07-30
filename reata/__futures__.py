"""Future MySQL data type objects to be added to the package."""

from typing import Optional, Union


__all__ = [
    "CHAR",
    "VARCHAR",
    "TINYTEXT",
    "TEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
    "BINARY",
    "VARBINARY",
    "BLOB",
    "ENUM",
    "SET",
    "DECIMAL",
    "FLOAT",
    "DOUBLE",
    "TINYINT",
    "SMALLINT",
    "MEDIUMINT",
    "INTEGER",
    "BIGINT",
    "YEAR",
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
]


class BaseDataType:
    """
    Base data type class from which all data types inheret their
    attributes and default methods.
    """

    def __init__(self, attrs: str = "") -> None:
        self.dtype = self.__class__.__name__
        self.attrs = attrs

    def __str__(self) -> str:
        return f"{self.dtype} {self.attrs}"

    def __repr__(self) -> str:
        return str(self)


class CharType(BaseDataType):
    """Base class for all character types."""

    def __init__(self, size: int, attrs: str = "") -> None:
        super().__init__(attrs)
        self.size = size
        self.attrs = attrs

    def __str__(self) -> str:
        return f"{self.dtype}({self.size}) {self.attrs}"


class StringType(BaseDataType):
    """Base class for all string types."""

    def __init__(self, size: Optional[int] = None, attrs: str = "") -> None:
        super().__init__(attrs)
        self.size = size
        self.attrs = attrs

    def __str__(self) -> str:
        if self.size is None:
            return f"{self.dtype} {self.attrs}"
        else:
            return f"{self.dtype}({self.size}) {self.attrs}"


class DecimalType(BaseDataType):
    """Base class for all floating point types."""

    def __init__(self, size: int, precision: int, attrs: str = "") -> None:
        super().__init__(attrs)
        self.size = size
        self.precision = precision

    def __str__(self) -> str:
        return f"{self.dtype}({self.size}, {self.precision}) {self.attrs}"

    @property
    def effective_size(self) -> int:
        """
        Returns the maximum size of values which can be stored in the column
        without producing an error. Rounding is not taken into
        consideration. Thus, floating point values whose precision is higher
        than the precision specified for the column will ultimately still be
        rounded by SQL.
        """
        return self.size - self.precision


class IntegerType(BaseDataType):
    """Base class for all integer types."""

    def __init__(self, size: int, attrs: str = "") -> None:
        super().__init__(attrs)
        self.size = size

    def __str__(self) -> str:
        return f"{self.dtype}({self.size}) {self.attrs}"


class TimeType(BaseDataType):
    """Base class for all time types that take a precision argument."""

    def __init__(self, precision: int = None, attrs: str = "") -> None:
        super().__init__(attrs)
        self.precision = precision

    def __str__(self) -> str:
        if self.precision is not None:
            return f"{self.dtype}({self.precision}) {self.attrs}"
        else:
            return f"{self.dtype} {self.attrs}"


class CHAR(CharType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class VARCHAR(CharType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class BINARY(CharType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class VARBINARY(CharType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class TINYTEXT(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class TEXT(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class MEDIUMTEXT(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class LONGTEXT(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class BLOB(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class ENUM(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class SET(StringType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class DECIMAL(DecimalType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class FLOAT(DecimalType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class DOUBLE(DecimalType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class TINYINT(IntegerType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class SMALLINT(IntegerType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class MEDIUMINT(IntegerType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class INTEGER(IntegerType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class BIGINT(IntegerType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class YEAR(BaseDataType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class DATE(BaseDataType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class DATETIME(BaseDataType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class TIME(BaseDataType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


class TIMESTAMP(BaseDataType):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
