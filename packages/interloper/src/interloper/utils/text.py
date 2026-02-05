"""Text utilities."""

import re


def to_display(text: str) -> str:
    """Convert text to display name."""
    if not text:
        return ""

    # separators → space
    text = re.sub(r"[_\-]+", " ", text)

    # camelCase / PascalCase → space
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)

    # collapse whitespace and capitalize
    return " ".join(text.split()).title()
