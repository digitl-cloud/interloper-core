"""Text utilities."""

import re

_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")


def validate_name(name: str) -> None:
    """Validate that *name* contains only letters, numbers, and underscores.

    Must start with a letter.

    Raises:
        ValueError: If the name is invalid.
    """
    if not _NAME_RE.match(name):
        raise ValueError(
            f"Name '{name}' is invalid. "
            "Names must start with a letter and contain only letters, numbers, and underscores."
        )


def slugify(text: str) -> str:
    """Convert text to a URL/key-safe slug.

    - Lowercases the text
    - Converts camelCase/PascalCase to hyphen-separated
    - Replaces underscores and spaces with hyphens
    - Collapses consecutive hyphens
    - Strips leading/trailing hyphens
    """
    if not text:
        return ""

    # camelCase / PascalCase → hyphen
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1-\2", text)
    # lowercase
    text = text.lower()
    # replace underscores and spaces with hyphens
    text = re.sub(r"[_\s]+", "-", text)
    # collapse consecutive hyphens
    text = re.sub(r"-+", "-", text)
    # strip leading/trailing hyphens
    return text.strip("-")


def to_label(text: str) -> str:
    """Convert text to a human-readable label."""
    if not text:
        return ""

    # separators → space
    text = re.sub(r"[_\-]+", " ", text)
    # camelCase / PascalCase → space
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    # collapse whitespace and capitalize
    return " ".join(text.split()).title()
