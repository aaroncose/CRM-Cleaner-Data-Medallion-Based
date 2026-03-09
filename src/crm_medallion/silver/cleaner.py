"""Data cleaner for the Silver Layer."""

from typing import Any

from crm_medallion.silver.models import RawRecord, CleanedRecord
from crm_medallion.silver.rules import CleaningRule, ConsistencyChecker
from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class DataCleaner:
    """Applies basic data cleaning transformations."""

    def __init__(self, rules: list[CleaningRule] | None = None):
        """
        Initialize with cleaning rules.

        Args:
            rules: List of cleaning rules to apply. If None, no rules are applied.
        """
        self.rules = rules or []
        self.consistency_checker = ConsistencyChecker()

    def clean(self, record: RawRecord) -> CleanedRecord:
        """
        Apply cleaning rules to a record.

        Args:
            record: The raw record to clean

        Returns:
            CleanedRecord with transformations applied
        """
        cleaned_data: dict[str, Any] = {}
        cleaning_log: list[str] = []

        for field_name, value in record.data.items():
            current_value = value

            for rule in self.rules:
                if rule.applies_to(field_name):
                    new_value, log_msg = rule.clean(current_value, field_name)
                    if log_msg:
                        cleaning_log.append(log_msg)
                    current_value = new_value

            cleaned_data[field_name] = current_value

        consistency_warnings = self.consistency_checker.check_consistency(cleaned_data)
        for warning in consistency_warnings:
            cleaning_log.append(f"WARNING: {warning}")

        return CleanedRecord(
            row_number=record.row_number,
            data=cleaned_data,
            cleaning_log=cleaning_log,
            source_dataset_id=record.source_dataset_id,
        )

    def clean_batch(self, records: list[RawRecord]) -> list[CleanedRecord]:
        """
        Clean multiple records.

        Args:
            records: List of raw records to clean

        Returns:
            List of cleaned records
        """
        return [self.clean(record) for record in records]

    def register_rule(self, rule: CleaningRule) -> None:
        """
        Register a custom cleaning rule.

        Args:
            rule: The cleaning rule to register
        """
        self.rules.append(rule)
        logger.debug(f"Registered cleaning rule: {rule.__class__.__name__}")
