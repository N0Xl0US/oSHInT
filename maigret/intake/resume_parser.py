"""Resume parsing helpers for resume-first identity intake."""

from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
import re
from urllib.parse import unquote, urlsplit, urlunsplit

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_EMAIL_EXACT_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_MAILTO_RE = re.compile(r"mailto:([^\s<>\"\')\]]+)", re.IGNORECASE)
_PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
_HANDLE_RE = re.compile(r"(?:^|\s)@([A-Za-z0-9_.-]{2,40})")
_TEXT_URL_RE = re.compile(r"https?://[^\s<>\"\)\]]+", re.IGNORECASE)
_URL_HANDLE_RE = re.compile(
    r"https?://(?:www\.)?(?:github\.com|twitter\.com|x\.com|instagram\.com|reddit\.com|linkedin\.com/in)/([A-Za-z0-9_.-]{2,60})",
    re.IGNORECASE,
)
_NAME_LINE_RE = re.compile(r"^[A-Za-z][A-Za-z .'-]{2,80}$")
_SKIP_NAME_TERMS = {
    "resume",
    "curriculum vitae",
    "cv",
    "email",
    "phone",
    "address",
    "experience",
    "education",
    "skills",
    "summary",
    "objective",
}


class ResumeParseError(ValueError):
    """Raised when a resume cannot be parsed into usable text."""


@dataclass(frozen=True)
class ResumeExtractedLink:
    """A normalized hyperlink extracted from resume content."""

    url: str
    platform: str
    source: str


@dataclass
class ResumeParseResult:
    """Structured signals extracted from a resume document."""

    name: str | None
    emails: list[str]
    phones: list[str]
    usernames: list[str]
    institutions: list[str]
    text_length: int
    links: list[ResumeExtractedLink] = field(default_factory=list)


def parse_resume_bytes(
    content: bytes,
    filename: str | None,
    content_type: str | None = None,
) -> ResumeParseResult:
    """Parse a PDF or DOCX resume and extract identity hints."""
    if not content:
        raise ResumeParseError("Uploaded resume is empty")

    extension = _resolve_extension(filename=filename, content_type=content_type)
    if extension == ".pdf":
        text, embedded_urls = _extract_pdf_content(content)
    elif extension == ".docx":
        text, embedded_urls = _extract_docx_content(content)
    else:
        raise ResumeParseError("Unsupported resume type. Upload a PDF or DOCX file")

    text = _normalize_text(text)
    links = _extract_links(text=text, embedded_urls=embedded_urls)
    if not text and not links:
        raise ResumeParseError("Unable to extract readable text from resume")

    emails = _extract_emails(text=text, embedded_urls=embedded_urls)
    phones = sorted({p for p in (_normalize_phone(raw) for raw in _PHONE_RE.findall(text)) if p})
    usernames = _extract_usernames(text=text, emails=emails, links=links)
    institutions = _extract_institutions(text)

    return ResumeParseResult(
        name=_extract_name(text),
        emails=emails,
        phones=phones,
        usernames=usernames,
        institutions=institutions,
        text_length=len(text),
        links=links,
    )


def _resolve_extension(filename: str | None, content_type: str | None) -> str:
    if filename and "." in filename:
        return "." + filename.lower().rsplit(".", 1)[1]

    content_type = (content_type or "").lower()
    if "pdf" in content_type:
        return ".pdf"
    if "word" in content_type or "docx" in content_type:
        return ".docx"
    return ""


def _extract_pdf_content(content: bytes) -> tuple[str, list[str]]:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise ResumeParseError("Missing parser dependency: install pypdf") from exc

    try:
        reader = PdfReader(BytesIO(content))
    except Exception as exc:
        raise ResumeParseError("Uploaded PDF could not be opened") from exc

    pages: list[str] = []
    embedded_urls: list[str] = []
    for page in reader.pages:
        try:
            pages.append(page.extract_text() or "")
        except Exception:
            pages.append("")
        embedded_urls.extend(_extract_pdf_page_hyperlinks(page))
    return "\n".join(pages), embedded_urls


def _extract_docx_content(content: bytes) -> tuple[str, list[str]]:
    try:
        from docx import Document
    except ImportError as exc:
        raise ResumeParseError("Missing parser dependency: install python-docx") from exc

    try:
        document = Document(BytesIO(content))
    except Exception as exc:
        raise ResumeParseError("Uploaded DOCX could not be opened") from exc

    lines = [paragraph.text.strip() for paragraph in document.paragraphs if paragraph.text and paragraph.text.strip()]
    return "\n".join(lines), _extract_docx_hyperlinks(document)


def _extract_pdf_page_hyperlinks(page: object) -> list[str]:
    urls: list[str] = []
    annots = page.get("/Annots") if hasattr(page, "get") else None
    if not annots:
        return urls

    for annot_ref in annots:
        try:
            annot = annot_ref.get_object()
        except Exception:
            continue

        if not hasattr(annot, "get"):
            continue

        action = annot.get("/A")
        if action is not None and hasattr(action, "get_object"):
            try:
                action = action.get_object()
            except Exception:
                action = None

        if hasattr(action, "get"):
            uri = action.get("/URI")
            if uri:
                urls.append(str(uri))

        direct_uri = annot.get("/URI")
        if direct_uri:
            urls.append(str(direct_uri))

    return urls


def _extract_docx_hyperlinks(document: object) -> list[str]:
    urls: list[str] = []

    part = getattr(document, "part", None)
    rels = getattr(part, "rels", None)
    if not rels:
        return urls

    for rel in rels.values():
        reltype = str(getattr(rel, "reltype", ""))
        if not reltype.endswith("/hyperlink"):
            continue

        target = getattr(rel, "target_ref", None)
        if isinstance(target, str) and target.strip():
            urls.append(target.strip())

    return urls


def _extract_links(text: str, embedded_urls: list[str]) -> list[ResumeExtractedLink]:
    sources_by_url: dict[str, set[str]] = {}

    for match in _TEXT_URL_RE.findall(text):
        normalized = _normalize_url(match)
        if normalized:
            sources_by_url.setdefault(normalized, set()).add("visible_text")

    for value in embedded_urls:
        normalized = _normalize_url(value)
        if normalized:
            sources_by_url.setdefault(normalized, set()).add("embedded")

    links: list[ResumeExtractedLink] = []
    for url in sorted(sources_by_url):
        source = "embedded" if "embedded" in sources_by_url[url] else "visible_text"
        links.append(
            ResumeExtractedLink(
                url=url,
                platform=_classify_platform(url),
                source=source,
            )
        )

    return links


def _extract_emails(text: str, embedded_urls: list[str]) -> list[str]:
    emails: set[str] = set()

    for match in _EMAIL_RE.findall(text):
        normalized = _normalize_email_candidate(match)
        if normalized:
            emails.add(normalized)

    deobfuscated = _deobfuscate_email_text(text)
    if deobfuscated != text:
        for match in _EMAIL_RE.findall(deobfuscated):
            normalized = _normalize_email_candidate(match)
            if normalized:
                emails.add(normalized)

    for candidate in _MAILTO_RE.findall(text):
        normalized = _normalize_email_candidate(candidate)
        if normalized:
            emails.add(normalized)

    for value in embedded_urls:
        lowered = value.lower()
        if not lowered.startswith("mailto:"):
            continue
        normalized = _normalize_email_candidate(value[7:])
        if normalized:
            emails.add(normalized)

    return sorted(emails)


def _deobfuscate_email_text(text: str) -> str:
    transformed = text

    substitutions = (
        (r"\[\s*at\s*\]|\(\s*at\s*\)", "@"),
        (r"\[\s*dot\s*\]|\(\s*dot\s*\)", "."),
        (r"\s+at\s+", "@"),
        (r"\s+dot\s+", "."),
    )

    for pattern, replacement in substitutions:
        transformed = re.sub(pattern, replacement, transformed, flags=re.IGNORECASE)

    return transformed


def _normalize_email_candidate(value: str) -> str | None:
    candidate = unquote((value or "").strip())
    if not candidate:
        return None

    candidate = candidate.split("?", 1)[0].strip().strip(".,;:!?|)]}>'\"")
    candidate = candidate.lower()
    if not candidate:
        return None

    if not _EMAIL_EXACT_RE.fullmatch(candidate):
        return None

    return candidate


def _normalize_url(value: str) -> str | None:
    cleaned = _clean_url_candidate(value)
    if not cleaned:
        return None

    # Some resume hyperlink targets are stored as bare domains/paths
    # (for example linkedin.com/in/handle) without an explicit scheme.
    if re.match(r"^(?:www\.)?[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?:/.*)?$", cleaned, re.IGNORECASE):
        cleaned = f"https://{cleaned.lstrip('/')}"

    try:
        parts = urlsplit(cleaned)
    except Exception:
        return None

    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        return None

    host = parts.netloc.lower().strip()
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]

    path = parts.path.rstrip("/")
    return urlunsplit((scheme, host, path, parts.query, ""))


def _clean_url_candidate(value: str) -> str:
    cleaned = value.strip()
    cleaned = cleaned.lstrip("([<{\"'")
    cleaned = cleaned.rstrip(".,;:!?|)]}>\"'")
    return cleaned.strip()


def _classify_platform(url: str) -> str:
    host = urlsplit(url).netloc.lower()
    if host.endswith("github.com"):
        return "github"
    if host.endswith("linkedin.com"):
        return "linkedin"
    if host.endswith("twitter.com") or host.endswith("x.com"):
        return "x"
    if host.endswith("reddit.com"):
        return "reddit"
    if host.endswith("instagram.com"):
        return "instagram"
    return "unknown"


def _normalize_text(text: str) -> str:
    collapsed_lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in collapsed_lines if line)


def _extract_name(text: str) -> str | None:
    for line in text.splitlines()[:12]:
        lower = line.lower().strip()
        if not line or "@" in line or "http" in lower:
            continue
        if any(term in lower for term in _SKIP_NAME_TERMS):
            continue
        if not _NAME_LINE_RE.fullmatch(line.strip()):
            continue

        tokens = [tok for tok in re.split(r"\s+", line.strip()) if tok]
        tokens = _repair_split_name_tokens(tokens)
        if 2 <= len(tokens) <= 4 and all(any(ch.isalpha() for ch in tok) for tok in tokens):
            return " ".join(tokens)
    return None


def _repair_split_name_tokens(tokens: list[str]) -> list[str]:
    """
    Merge OCR/PDF artifacts like "S econdname" into "Secondname".

    Only merges when a single-letter alpha token is followed by a token that
    starts with lowercase (to avoid changing initials like "J. R. R.").
    """
    repaired: list[str] = []
    idx = 0

    while idx < len(tokens):
        current = tokens[idx]
        nxt = tokens[idx + 1] if idx + 1 < len(tokens) else ""

        if (
            len(current) == 1
            and current.isalpha()
            and nxt
            and nxt[0].islower()
        ):
            repaired.append(f"{current}{nxt}")
            idx += 2
            continue

        repaired.append(current)
        idx += 1

    return repaired


def _normalize_phone(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None

    has_plus = value.startswith("+")
    digits = re.sub(r"\D", "", value)
    if len(digits) < 10 or len(digits) > 15:
        return None
    return f"+{digits}" if has_plus else digits


def _extract_usernames(text: str, emails: list[str], links: list[ResumeExtractedLink]) -> list[str]:
    candidates: set[str] = set()

    for local in (email.split("@", 1)[0] for email in emails):
        cleaned = _clean_handle(local)
        if cleaned:
            candidates.add(cleaned)

    for match in _HANDLE_RE.findall(text):
        cleaned = _clean_handle(match)
        if cleaned:
            candidates.add(cleaned)

    for match in _URL_HANDLE_RE.findall(text):
        cleaned = _clean_handle(match)
        if cleaned:
            candidates.add(cleaned)

    for link in links:
        for match in _URL_HANDLE_RE.findall(link.url):
            cleaned = _clean_handle(match)
            if cleaned:
                candidates.add(cleaned)

    return sorted(candidates)


def _clean_handle(value: str) -> str | None:
    candidate = value.strip().lstrip("@").lower()
    if len(candidate) < 3:
        return None
    if not re.fullmatch(r"[a-z0-9_.-]{3,40}", candidate):
        return None
    if candidate in {"resume", "profile", "linkedin", "github", "twitter"}:
        return None
    return candidate


def _extract_institutions(text: str) -> list[str]:
    institutions: list[str] = []
    seen: set[str] = set()
    for line in text.splitlines()[:120]:
        lower = line.lower()
        if not any(key in lower for key in ("university", "college", "institute", "laboratory", "labs", "school")):
            continue

        normalized = re.sub(r"\s+", " ", line).strip()
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        institutions.append(normalized)

        if len(institutions) >= 5:
            break

    return institutions
