"""Identity intake utilities (resume parsing and normalization)."""

from maigret.intake.resume_parser import (
    ResumeExtractedLink,
    ResumeParseError,
    ResumeParseResult,
    parse_resume_bytes,
)

__all__ = [
    "ResumeExtractedLink",
    "ResumeParseError",
    "ResumeParseResult",
    "parse_resume_bytes",
]
