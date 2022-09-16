from dataclasses import fields
from datetime import datetime
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
    elif field_type is datetime:
        return 'datetime'
    else:
        return 'string'


def _parse_cypher_properties(properties: dict[str, Any],
                             model_cls: 'akiradb.model.base_model.MetaModel'):
    for model_field in fields(model_cls):
        if model_field.name in model_cls._properties_names and model_field.name not in properties:
            properties[model_field.name] = None

    return model_cls(**properties)
