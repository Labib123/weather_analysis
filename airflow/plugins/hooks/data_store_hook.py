from airflow.hooks.base_hook import BaseHook
from data_store_client import DataStoreClient


class DataStoreHook(BaseHook):
    """
    Airflow hook for interacting with a data store.
    """

    def __init__(self, conn_id):
        """
        Constructor that takes a connection ID as an argument.
        """
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        """
        Retrieves a connection to the data store.
        """
        conn = self.get_connection(self.conn_id)
        client = DataStoreClient(
            {
                "host": conn.host,
                "port": conn.port,
                "username": conn.login,
                "password": conn.password,
                "database": conn.schema,
            }
        )
        return client
