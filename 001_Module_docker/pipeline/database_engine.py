"""
Docstring pour pipeline.database_engine
"""

from sqlalchemy import create_engine


class DatabaseEngine():
    def __init__(self, db_supplier: str, host: str, port: str,
                 user: str, pwd: str, db: str, ):
        self.engine = create_engine(
            f'{db_supplier}://{user}:{pwd}@{host}:{port}/{db}')
