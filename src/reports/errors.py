"""Custom exceptions for report generation."""

from __future__ import annotations


class ReportError(Exception):
    """Raised when a report cannot be generated."""

    def __init__(self, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.code = code


__all__ = ['ReportError']
