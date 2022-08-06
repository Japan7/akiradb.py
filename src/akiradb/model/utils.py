from datetime import date, datetime
from typing import Any, Callable, Tuple, TypeVar, Union

_T = TypeVar('_T')


def __dataclass_transform__(
    *,
    eq_default: bool = True,
    order_default: bool = False,
    kw_only_default: bool = False,
    field_specifiers: Tuple[Union[type, Callable[..., Any]], ...] = (()),
) -> Callable[[_T], _T]:
    # If used within a stub file, the following implementation can be
    # replaced with "...".
    return lambda a: a


def _get_cypher_property_type(field_type):
    if field_type is int:
        return 'integer'
    elif field_type is bool:
        return 'boolean'
    elif field_type is float:
        return 'double'
    elif field_type is bytes:
        return 'byte'
    elif field_type is date:
        return 'date'
    elif field_type is datetime:
        return 'datetime'
    else:
        return 'string'


def _get_cypher_value(value):
    from akiradb.model.proxies import PropertyChangesRecorder
    if isinstance(value, PropertyChangesRecorder):
        value = value.value
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    elif isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    else:
        return repr(value)
