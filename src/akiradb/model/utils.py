from dataclasses import fields
from datetime import date, datetime
from types import NoneType, UnionType
from typing import Any, Callable, Tuple, TypeVar, Union, get_args, get_origin

import akiradb

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
    if get_origin(field_type) in [Union, UnionType]:
        field_type = [t for t in get_args(field_type) if t is not NoneType][0]

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

    if value is None:
        return "null"
    elif isinstance(value, date):
        return f"'{value.isoformat()}'"
    elif isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    else:
        return repr(value)


def _parse_cypher_properties(properties: dict[str, Any],
                             model_cls: 'akiradb.model.base_model.MetaModel'):
    for model_field in fields(model_cls):
        if model_field.name in model_cls._properties_names:
            if model_field.name in properties:
                parsed_value = properties[model_field.name]

                if parsed_value == '  cypher.null':
                    parsed_value = None
                if parsed_value:
                    if model_field.type is date:
                        parsed_value = date.fromtimestamp(parsed_value//1000)
                    elif model_field.type is datetime:
                        parsed_value = datetime.fromtimestamp(parsed_value//1000)

                properties[model_field.name] = parsed_value
            else:
                properties[model_field.name] = None

    return model_cls(**properties)
