"""Data cleaner for the Silver Layer."""

import math
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

        # Calculate importe_pendiente if missing or NaN
        cleaned_data, pendiente_log = self._calculate_importe_pendiente(cleaned_data)
        if pendiente_log:
            cleaning_log.append(pendiente_log)

        consistency_warnings = self.consistency_checker.check_consistency(cleaned_data)
        for warning in consistency_warnings:
            cleaning_log.append(f"WARNING: {warning}")

        return CleanedRecord(
            row_number=record.row_number,
            data=cleaned_data,
            cleaning_log=cleaning_log,
            source_dataset_id=record.source_dataset_id,
        )

    def _calculate_importe_pendiente(
        self, data: dict[str, Any]
    ) -> tuple[dict[str, Any], str | None]:
        """
        Calculate importe_pendiente based on estado_factura and importe_total.

        Rules:
        - Pagada → 0.0
        - Pendiente or Vencida → importe_total
        - Parcialmente pagada → importe_total * 0.5

        Args:
            data: The cleaned record data

        Returns:
            Tuple of (modified_data, log_message or None)
        """
        importe_pendiente = data.get("importe_pendiente")
        estado = data.get("estado_factura", "")
        importe_total = data.get("importe_total")

        # Check if importe_pendiente needs to be calculated
        needs_calculation = (
            importe_pendiente is None
            or importe_pendiente == ""
            or (isinstance(importe_pendiente, float) and math.isnan(importe_pendiente))
        )

        if needs_calculation and importe_total is not None:
            try:
                total = float(importe_total)
                estado_lower = str(estado).lower().strip()

                if "pagada" in estado_lower and "parcialmente" not in estado_lower:
                    # Pagada → 0.0
                    data["importe_pendiente"] = 0.0
                    return data, f"Calculated importe_pendiente=0.0 (estado={estado})"

                elif "pendiente" in estado_lower or "vencida" in estado_lower:
                    # Pendiente or Vencida → importe_total
                    data["importe_pendiente"] = total
                    return data, f"Calculated importe_pendiente={total:.2f} (estado={estado})"

                elif "parcialmente" in estado_lower:
                    # Parcialmente pagada → importe_total * 0.5
                    pending = total * 0.5
                    data["importe_pendiente"] = pending
                    return data, f"Calculated importe_pendiente={pending:.2f} (estado={estado})"

                else:
                    # Unknown estado, set to 0.0 to avoid NaN
                    data["importe_pendiente"] = 0.0
                    return data, f"Set importe_pendiente=0.0 (unknown estado: {estado})"

            except (ValueError, TypeError):
                # Can't calculate, set to 0.0 to avoid NaN
                data["importe_pendiente"] = 0.0
                return data, "Set importe_pendiente=0.0 (could not calculate)"

        # If importe_pendiente exists but is NaN, replace with 0.0
        if isinstance(importe_pendiente, float) and math.isnan(importe_pendiente):
            data["importe_pendiente"] = 0.0
            return data, "Replaced NaN importe_pendiente with 0.0"

        return data, None

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
