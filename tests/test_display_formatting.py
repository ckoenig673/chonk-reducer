from chonk_reducer.core.display_formatting import format_optional_percent


def test_format_optional_percent_with_numeric_value() -> None:
    assert format_optional_percent(12.345, decimals=1, default='-') == '12.3%'


def test_format_optional_percent_with_none_uses_default() -> None:
    assert format_optional_percent(None, decimals=1, default='-') == '-'


def test_format_optional_percent_with_invalid_value_uses_default() -> None:
    assert format_optional_percent('nope', decimals=2, default='n/a') == 'n/a'
