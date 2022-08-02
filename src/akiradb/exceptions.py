class AkiraNotConnectedException(Exception):
    def __init__(self):
        super().__init__('Database is not connected')


class AkiraUnknownNodeException(Exception):
    def __init__(self):
        super().__init__('Attempted to load an unknown node')


class AkiraNodeNotFoundException(Exception):
    def __init__(self):
        super().__init__('Requested node could not be found')
