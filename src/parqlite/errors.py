class ParqliteError(Exception):
    """Base exception for parqlite."""


class TableAlreadyExistsError(ParqliteError):
    """Raised when creating a table that already exists."""


class TableNotFoundError(ParqliteError):
    """Raised when a requested table does not exist."""


class NamespaceAlreadyExistsError(ParqliteError):
    """Raised when creating a namespace that already exists."""


class NamespaceNotFoundError(ParqliteError):
    """Raised when a requested namespace does not exist."""


class SchemaError(ParqliteError):
    """Raised when a declared schema is invalid."""


class SchemaMismatchError(ParqliteError):
    """Raised when input data does not match a table schema."""


class PartitionError(ParqliteError):
    """Raised when a partition spec is invalid."""


class InputDataError(ParqliteError):
    """Raised when input data cannot be read."""


class QueryBackendError(ParqliteError):
    """Raised when the SQL query backend cannot be initialized or used."""


class SnapshotError(ParqliteError):
    """Raised when snapshot selection or maintenance fails."""


class OrphanFileError(ParqliteError):
    """Raised when orphan file inspection or removal fails."""
