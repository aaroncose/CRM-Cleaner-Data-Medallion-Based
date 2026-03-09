"""Cleaning rules for the Silver Layer."""

import re
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from crm_medallion.utils.logging import get_logger

logger = get_logger(__name__)


class CleaningRule(ABC):
    """Abstract base class for cleaning rules."""

    @abstractmethod
    def applies_to(self, field_name: str) -> bool:
        """Check if this rule applies to the given field."""
        pass

    @abstractmethod
    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        """
        Clean the value.

        Args:
            value: The value to clean
            field_name: The name of the field

        Returns:
            Tuple of (cleaned_value, log_message or None)
        """
        pass


class WhitespaceStripper(CleaningRule):
    """Remove leading and trailing whitespace from text fields."""

    TEXT_FIELDS = {
        "proveedor", "categoria", "estado_factura", "tipo",
        "nif_cif", "num_factura", "nombre", "descripcion",
    }

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() in self.TEXT_FIELDS or field_name.lower().endswith("_str")

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str):
            return value, None

        stripped = value.strip()
        if stripped != value:
            return stripped, f"Stripped whitespace from {field_name}"
        return value, None


class CurrencyNormalizer(CleaningRule):
    """Normalize currency values: remove symbols, fix decimal separators."""

    NUMERIC_FIELDS = {"importe_base", "iva", "importe_total", "importe_pendiente"}

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() in self.NUMERIC_FIELDS

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if value is None or value == "":
            return value, None

        if isinstance(value, (int, float)):
            return float(value), None

        if not isinstance(value, str):
            return value, None

        original = value
        cleaned = value.strip()

        cleaned = re.sub(r"[€$]", "", cleaned)
        cleaned = re.sub(r"\s*(EUR|USD|eur|usd)\s*", "", cleaned)
        cleaned = cleaned.strip()

        if re.match(r"^\d{1,3}(\.\d{3})*(,\d+)?$", cleaned):
            cleaned = cleaned.replace(".", "")
            cleaned = cleaned.replace(",", ".")
        elif "," in cleaned and "." not in cleaned:
            cleaned = cleaned.replace(",", ".")
        elif "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "")
                cleaned = cleaned.replace(",", ".")

        cleaned = re.sub(r"[^\d.\-]", "", cleaned)

        try:
            result = float(cleaned) if cleaned else None
            if str(original) != str(result):
                return result, f"Normalized currency in {field_name}: '{original}' -> {result}"
            return result, None
        except ValueError:
            return value, None


class DateNormalizer(CleaningRule):
    """Normalize date fields to ISO 8601 format (YYYY-MM-DD)."""

    DATE_FIELDS = {"fecha", "fecha_factura", "fecha_pago", "fecha_vencimiento"}

    SPANISH_MONTHS = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() in self.DATE_FIELDS

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if value is None or value == "":
            return value, None

        if isinstance(value, datetime):
            return value, None

        if not isinstance(value, str):
            return value, None

        original = value.strip()
        parsed_date = None

        patterns = [
            (r"^(\d{4})-(\d{1,2})-(\d{1,2})$", lambda m: (int(m[1]), int(m[2]), int(m[3]))),
            (r"^(\d{1,2})/(\d{1,2})/(\d{4})$", lambda m: (int(m[3]), int(m[2]), int(m[1]))),
            (r"^(\d{1,2})-(\d{1,2})-(\d{4})$", lambda m: (int(m[3]), int(m[2]), int(m[1]))),
            (r"^(\d{1,2})/(\d{1,2})/(\d{2})$", lambda m: (2000 + int(m[3]) if int(m[3]) < 50 else 1900 + int(m[3]), int(m[2]), int(m[1]))),
            (r"^(\d{1,2})-(\d{1,2})-(\d{2})$", lambda m: (2000 + int(m[3]) if int(m[3]) < 50 else 1900 + int(m[3]), int(m[2]), int(m[1]))),
        ]

        for pattern, extractor in patterns:
            match = re.match(pattern, original)
            if match:
                try:
                    year, month, day = extractor(match)
                    parsed_date = datetime(year, month, day)
                    break
                except ValueError:
                    continue

        if parsed_date is None:
            spanish_pattern = r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})"
            match = re.match(spanish_pattern, original, re.IGNORECASE)
            if match:
                day = int(match.group(1))
                month_name = match.group(2).lower()
                year = int(match.group(3))
                month = self.SPANISH_MONTHS.get(month_name)
                if month:
                    try:
                        parsed_date = datetime(year, month, day)
                    except ValueError:
                        pass

        if parsed_date:
            return parsed_date, f"Normalized date in {field_name}: '{original}' -> {parsed_date.strftime('%Y-%m-%d')}"

        return value, None


class CaseNormalizer(CleaningRule):
    """Normalize text fields to Title Case."""

    TEXT_FIELDS = {"proveedor", "categoria", "estado_factura"}

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() in self.TEXT_FIELDS

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str) or not value:
            return value, None

        original = value
        normalized = value.strip().title()

        if normalized != original:
            return normalized, f"Normalized case in {field_name}: '{original}' -> '{normalized}'"
        return value, None


class InvoiceNumberNormalizer(CleaningRule):
    """Normalize invoice numbers to FAC-YYYY-NNNN format."""

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() == "num_factura"

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str) or not value:
            return value, None

        original = value.strip()
        cleaned = original.upper()

        cleaned = re.sub(r"[/\s]+", "-", cleaned)

        match = re.match(r"FAC-?(\d{4})-?(\d+)", cleaned)
        if match:
            year = match.group(1)
            number = match.group(2).zfill(4)
            normalized = f"FAC-{year}-{number}"
            if normalized != original:
                return normalized, f"Normalized invoice number: '{original}' -> '{normalized}'"
            return original, None

        match = re.match(r"(\d{4})-?(\d+)", cleaned)
        if match:
            year = match.group(1)
            number = match.group(2).zfill(4)
            normalized = f"FAC-{year}-{number}"
            return normalized, f"Normalized invoice number: '{original}' -> '{normalized}'"

        return original, None


class NifCifNormalizer(CleaningRule):
    """Normalize NIF/CIF to standard format (letter + 8 digits or 8 digits + letter)."""

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() == "nif_cif"

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str) or not value:
            return value, None

        original = value.strip()
        cleaned = re.sub(r"[\s\-]", "", original).upper()

        if re.match(r"^[A-Z]\d{8}$", cleaned):
            if cleaned != original:
                return cleaned, f"Normalized NIF/CIF: '{original}' -> '{cleaned}'"
            return original, None

        if re.match(r"^\d{8}[A-Z]$", cleaned):
            if cleaned != original:
                return cleaned, f"Normalized NIF/CIF: '{original}' -> '{cleaned}'"
            return original, None

        if re.match(r"^\d{8}$", cleaned):
            return cleaned, f"NIF/CIF missing letter: '{original}'"

        return original, None


class TipoNormalizer(CleaningRule):
    """Correct misspellings in tipo field (Ingreso/Gasto)."""

    INGRESO_VARIANTS = {
        "ingreso", "ingrso", "ingeso", "ingreos", "ingresso",
        "ingresos", "income", "entrada",
    }
    GASTO_VARIANTS = {
        "gasto", "gatso", "gsato", "gasot", "gastoo",
        "gastos", "expense", "salida",
    }

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() == "tipo"

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str) or not value:
            return value, None

        original = value.strip()
        lower_value = original.lower()

        if lower_value in self.INGRESO_VARIANTS:
            if original != "Ingreso":
                return "Ingreso", f"Corrected tipo: '{original}' -> 'Ingreso'"
            return original, None

        if lower_value in self.GASTO_VARIANTS:
            if original != "Gasto":
                return "Gasto", f"Corrected tipo: '{original}' -> 'Gasto'"
            return original, None

        return original, None


class EstadoFacturaNormalizer(CleaningRule):
    """Correct misspellings in estado_factura field."""

    CANONICAL_STATES = {
        "pagada": ["pagada", "pagda", "paga", "paid", "pagado"],
        "pendiente": ["pendiente", "pendiete", "pendientte", "pending", "pdte"],
        "vencida": ["vencida", "vencda", "vencido", "overdue"],
        "parcialmente pagada": [
            "parcialmente pagada", "parcial", "partial",
            "parcialmente", "parcialment pagada",
        ],
    }

    def applies_to(self, field_name: str) -> bool:
        return field_name.lower() == "estado_factura"

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        if not isinstance(value, str) or not value:
            return value, None

        original = value.strip()
        lower_value = original.lower()

        for canonical, variants in self.CANONICAL_STATES.items():
            if lower_value in variants:
                title_canonical = canonical.title()
                if original != title_canonical:
                    return title_canonical, f"Corrected estado_factura: '{original}' -> '{title_canonical}'"
                return original, None

        return original, None


class ConsistencyChecker(CleaningRule):
    """Check consistency between estado_factura and importe_pendiente."""

    def applies_to(self, field_name: str) -> bool:
        return False

    def clean(self, value: Any, field_name: str) -> tuple[Any, str | None]:
        return value, None

    def check_consistency(self, data: dict[str, Any]) -> list[str]:
        """
        Check for logical inconsistencies in the record.

        Returns:
            List of inconsistency warnings
        """
        warnings = []

        estado = data.get("estado_factura", "").lower() if isinstance(data.get("estado_factura"), str) else ""
        importe_pendiente = data.get("importe_pendiente")

        if importe_pendiente is not None:
            try:
                pendiente = float(importe_pendiente) if not isinstance(importe_pendiente, (int, float)) else importe_pendiente

                if estado == "pagada" and pendiente > 0:
                    warnings.append(
                        f"Inconsistency: estado_factura='Pagada' but importe_pendiente={pendiente} > 0"
                    )

                if estado == "pendiente" and pendiente == 0:
                    warnings.append(
                        f"Inconsistency: estado_factura='Pendiente' but importe_pendiente=0"
                    )
            except (ValueError, TypeError):
                pass

        return warnings


def get_default_cleaning_rules() -> list[CleaningRule]:
    """Get the default set of cleaning rules."""
    return [
        WhitespaceStripper(),
        CurrencyNormalizer(),
        DateNormalizer(),
        TipoNormalizer(),
        EstadoFacturaNormalizer(),
        NifCifNormalizer(),
        InvoiceNumberNormalizer(),
        CaseNormalizer(),
    ]
