import datetime
import logging

import googleapiclient.discovery
import os

from alpaca_trade_api.rest import REST

from azure.storage.queue import QueueClient

from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity


class MainClass():
    def __init__(self):
        self.youtube_client = self.setup_youtube_client()
        self.transaction_queue_client = self.setup_transaction_queue()
        self.table_storage_client = self.setup_table()
        self.alpaca_api_client = REST()

    def setup_youtube_client(self):
        os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
        api_service_name = "youtube"
        api_version = "v3"
        developer_key = os.getenv("YOUTUBE_API_KEY")

        return googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=developer_key)

    def setup_transaction_queue(self):
        connection_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        q = "calcha-q"
        return QueueClient.from_connection_string(connection_str, q)

    def setup_table(self):
        connection_str = os.getenv("AZURE_TABLE_CONNECTION_STRING")
        account_name = os.getenv("AZURE_TABLE_ACCOUNT_NAME")
        account_key = os.getenv("AZURE_TABLE_ACCOUNT_KEY")
        return TableService(account_name=account_name, account_key=account_key)
