"""small_etl - ETL system for S3 CSV to PostgreSQL synchronization."""

from small_etl.application import ETLPipeline, PipelineResult
from small_etl.domain import AccountType, Asset, Direction, OffsetFlag, Trade
from small_etl.services import LoadResult, ValidationResult

__version__ = "0.1.0"

__all__ = [
    "ETLPipeline",
    "PipelineResult",
    "Asset",
    "Trade",
    "AccountType",
    "Direction",
    "OffsetFlag",
    "ValidationResult",
    "LoadResult",
]
