class ParquetDBError(Exception):
    """Base exception for parquetdb."""


class TableAlreadyExistsError(ParquetDBError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(ParquetDBError):
    """Raised when a requested table does not exist."""


class NamespaceAlreadyExistsError(ParquetDBError):
    """Raised when creating a namespace that already exists."""


class NamespaceNotFoundError(ParquetDBError):
    """Raised when a requested namespace does not exist."""


class SchemaError(ParquetDBError):
    """Raised when a declared schema is invalid."""


class SchemaMismatchError(ParquetDBError):
    """Raised when input data does not match a table schema."""


class PartitionError(ParquetDBError):
    """Raised when a partition spec is invalid."""


class InputDataError(ParquetDBError):
    """Raised when input data cannot be read."""


class QueryBackendError(ParquetDBError):
    """Raised when the SQL query backend cannot be initialized or used."""


class SnapshotError(ParquetDBError):
    """Raised when snapshot selection or maintenance fails."""


class OrphanFileError(ParquetDBError):
    """Raised when orphan file inspection or removal fails."""
