class AkiraNotConnectedException(Exception):
    def __init__(self):
        super().__init__('Database is not connected')

