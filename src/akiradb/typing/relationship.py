from typing import Annotated, Awaitable, List, TypeVar


Type = TypeVar('Type')

OneToOneRelationship = Annotated[Awaitable[Type], 'Relationship', 'OneToOne']
OneToManyRelationship = Annotated[Awaitable[List[Type]], 'Relationship', 'OneToMany']
ManyToOneRelationship = Annotated[Awaitable[Type], 'Relationship', 'ManyToOne']
ManyToManyRelationship = Annotated[Awaitable[List[Type]], 'Relationship', 'ManyToMany']

