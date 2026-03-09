"""Record parser for the Silver Layer."""

import csv
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from crm_medallion.bronze.models import BronzeDataset
from crm_medallion.silver.models import RawRecord
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class RecordParser:
    """Parses raw CSV data into structured records."""

    def __init__(self, chunk_size: int = 1000):
        """
        Initialize parser.

        Args:
            chunk_size: Number of rows to read at a time for large files
        """
        self.chunk_size = chunk_size

    def parse(self, bronze_dataset: BronzeDataset) -> Iterator[RawRecord]:
        """
        Parse Bronze data into raw records.

        Args:
            bronze_dataset: The Bronze dataset to parse

        Yields:
            RawRecord objects (not yet validated)
        """
        file_path = bronze_dataset.storage_path
        encoding = bronze_dataset.encoding

        logger.debug(f"Parsing records from {file_path.name}")

        with open(file_path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)

            for row_number, row in enumerate(reader, start=2):
                clean_row = {k.strip(): v for k, v in row.items() if k is not None}

                yield RawRecord(
                    row_number=row_number,
                    data=clean_row,
                    source_dataset_id=bronze_dataset.id,
                )

    def parse_chunked(
        self,
        bronze_dataset: BronzeDataset,
    ) -> Iterator[list[RawRecord]]:
        """
        Parse Bronze data into chunks of raw records.

        Use this for large files to control memory usage.

        Args:
            bronze_dataset: The Bronze dataset to parse

        Yields:
            Lists of RawRecord objects (chunks)
        """
        file_path = bronze_dataset.storage_path
        encoding = bronze_dataset.encoding

        logger.debug(f"Parsing records in chunks from {file_path.name}")

        for chunk_df in pd.read_csv(
            file_path,
            encoding=encoding,
            chunksize=self.chunk_size,
            dtype=str,
            keep_default_na=False,
        ):
            records = []
            start_row = chunk_df.index[0] + 2

            for idx, row in chunk_df.iterrows():
                row_number = int(idx) + 2
                data = {k.strip(): str(v) for k, v in row.to_dict().items()}

                records.append(
                    RawRecord(
                        row_number=row_number,
                        data=data,
                        source_dataset_id=bronze_dataset.id,
                    )
                )

            yield records
