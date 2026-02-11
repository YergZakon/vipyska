"""Parser registry. All parsers auto-register via the register_parser decorator."""

PARSER_REGISTRY = []


def register_parser(cls):
    """Decorator to register a parser class in the global registry."""
    PARSER_REGISTRY.append(cls)
    return cls
