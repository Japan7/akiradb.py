class AkiraNotConnectedException(Exception):
    def __init__(self):
        super().__init__('Database is not connected')


class AkiraNodeTypeAlreadyDefinedException(Exception):
    def __init__(self, node_type: str):
        super().__init__(f'Node Type {node_type} was already defined')


class AkiraUnknownNodeException(Exception):
    def __init__(self):
        super().__init__('Attempted to load an unknown node')


class AkiraNodeNotFoundException(Exception):
    def __init__(self):
        super().__init__('Requested node could not be found')
