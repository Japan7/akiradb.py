from typing import Any, Mapping
import sys

if sys.version_info >= (3, 11):
    from typing import LiteralString
else:
    from typing_extensions import LiteralString


Query = LiteralString
Params = Mapping[str, Any]


class Label():
    def __init__(self, label_name):
        self.label_name = label_name
