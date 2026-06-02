"""CSV file reading and writing."""

import csv
from pathlib import Path
from typing import Optional

from dataforge.exceptions import IOError as DataForgeIOError
from dataforge.types import CSVPath, TableData


class CSVReader:
    """Read CSV files safely with type preservation."""

    def __init__(
        self,
        encoding: str = "utf-8",
        delimiter: str = ",",
        quote_char: str = '"',
    ):
        """
        Initialize CSV reader.

        Args:
            encoding: File encoding
            delimiter: CSV delimiter
            quote_char: CSV quote character
        """
        self.encoding = encoding
        self.delimiter = delimiter
        self.quote_char = quote_char

    def read(self, path: CSVPath) -> TableData:
        """
        Read CSV file and return list of row dictionaries.

        Args:
            path: Path to CSV file

        Returns:
            List of row dictionaries (preserving string types)

        Raises:
            IOError: If file cannot be read
        """
        path = Path(path)

        if not path.exists():
            raise DataForgeIOError(
                f"CSV file not found: {path}",
                context={"path": str(path)},
                suggestion=f"Verify the file exists at {path.absolute()}",
            )

        try:
            rows = []
            with open(path, "r", encoding=self.encoding) as f:
                reader = csv.DictReader(
                    f,
                    delimiter=self.delimiter,
                    quotechar=self.quote_char,
                )
                if not reader.fieldnames:
                    raise DataForgeIOError(
                        "CSV file is empty or has no headers",
                        context={"path": str(path)},
                    )

                for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                    rows.append(row)

            return rows

        except DataForgeIOError:
            raise
        except Exception as e:
            raise DataForgeIOError(
                f"Failed to read CSV file: {e}",
                context={"path": str(path), "error": str(e)},
                suggestion="Check file format and encoding",
            )

    def read_rows(self, path: CSVPath, limit: Optional[int] = None) -> TableData:
        """
        Read CSV file with optional row limit.

        Args:
            path: Path to CSV file
            limit: Maximum rows to read (None = all)

        Returns:
            List of row dictionaries
        """
        rows = self.read(path)
        return rows[:limit] if limit else rows


class CSVWriter:
    """Write CSV files safely with atomic operations."""

    def __init__(
        self,
        encoding: str = "utf-8",
        delimiter: str = ",",
        quote_char: str = '"',
    ):
        """
        Initialize CSV writer.

        Args:
            encoding: File encoding
            delimiter: CSV delimiter
            quote_char: CSV quote character
        """
        self.encoding = encoding
        self.delimiter = delimiter
        self.quote_char = quote_char

    def write(self, path: CSVPath, data: TableData, overwrite: bool = False) -> int:
        """
        Write data to CSV file.

        Args:
            path: Path to output CSV file
            data: List of row dictionaries
            overwrite: Whether to overwrite existing file

        Returns:
            Number of rows written

        Raises:
            IOError: If write fails
        """
        path = Path(path)

        if path.exists() and not overwrite:
            raise DataForgeIOError(
                f"Output file already exists: {path}",
                context={"path": str(path)},
                suggestion="Use overwrite=True to replace existing file",
            )

        if not data:
            raise DataForgeIOError(
                "Cannot write empty data to CSV",
                context={"rows": 0},
            )

        try:
            # Write to temporary file first (atomic write)
            temp_path = path.with_suffix(path.suffix + ".tmp")

            with open(temp_path, "w", encoding=self.encoding, newline="") as f:
                fieldnames = list(data[0].keys())
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    quotechar=self.quote_char,
                )
                writer.writeheader()
                writer.writerows(data)

            # Atomic rename
            temp_path.replace(path)
            return len(data)

        except Exception as e:
            raise DataForgeIOError(
                f"Failed to write CSV file: {e}",
                context={"path": str(path), "rows": len(data)},
                suggestion="Check disk space and file permissions",
            )

    def append(self, path: CSVPath, row: dict) -> None:
        """
        Append a single row to CSV file.

        Args:
            path: Path to CSV file
            row: Row dictionary to append

        Raises:
            IOError: If append fails
        """
        path = Path(path)

        if not path.exists():
            raise DataForgeIOError(
                f"CSV file not found: {path}",
                context={"path": str(path)},
            )

        try:
            with open(path, "a", encoding=self.encoding, newline="") as f:
                fieldnames = list(row.keys())
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    delimiter=self.delimiter,
                    quotechar=self.quote_char,
                )
                writer.writerow(row)

        except Exception as e:
            raise DataForgeIOError(
                f"Failed to append to CSV file: {e}",
                context={"path": str(path)},
            )
