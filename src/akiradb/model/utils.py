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
    else:
        return 'string'
