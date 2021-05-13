import datetime
import logging

import azure.functions as func

import googleapiclient.discovery
import os, time

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


class PurchaseStonks(MainClass):
    def __init__(self):
        super().__init__()

    # message format: "{ticker}*{comment_id}*{transaction_type}*{author}"
    def get_next_message_from_queue(self):
        messages = self.transaction_queue_client.peek_messages()

        content = None
        ticker = None
        comment_id = None
        transaction_type = None
        author = None
        for peeked_message in messages:
            content = peeked_message['content'].split('*')
            if len(content) == 4:
                ticker = content[0]
                comment_id = content[1]
                transaction_type = content[2]
                author = content[3]
        return ticker, comment_id, transaction_type, author

    def delete_message_from_queue(self):
        message = self.transaction_queue_client.receive_message()
        if message:
            msg = message.content
            self.transaction_queue_client.delete_message(message.id, message.pop_receipt)
            logging.info('** Removed msg from queue: {msg}'.format(msg=msg))

    def execute_order(self, ticker, transaction_type):
        ticker = ticker.replace(" ", "")
        execute = self.alpaca_api_client.submit_order (
            symbol = ticker,
            qty = 1,
            side = transaction_type,
            type = 'market',
            time_in_force = 'day',
        )
        return execute

    '''
    while not rate limited:
        get next message from queue
        try purchasing. catch rate limit errors
        if purchased successfully, delete message from queue
        otherwise, end function and wait 1 minute
    '''
    def execute(self):
        logging.info('*** Purchase stonks ran')
        start = time.time()
        curr = time.time()        
        should_continue = True
        while (should_continue and (curr - start < 180)):
            ticker, comment_id, transaction_type, author = self.get_next_message_from_queue()
            if ticker and comment_id and transaction_type and author:
                if ticker.upper() != "BRK.A" or transaction_type == "sell":
                    try:
                        purchased = self.execute_order(ticker, transaction_type)
                        if purchased:
                            # insert into azure table to display
                            transaction = {
                                'PartitionKey': self.get_key(),
                                'RowKey': self.get_key(),
                                'TransactionType': transaction_type,
                                'Author': author,
                                'CommentId': comment_id,
                                'Ticker': ticker
                            }
                            self.table_storage_client.insert_entity('processedtransactions', transaction)

                            logging.info('** Stock {ticker}: {transaction_type} by {author} placed'.format(
                                ticker=ticker,
                                transaction_type=transaction_type,
                                author=author
                            ))
                            self.delete_message_from_queue()
                    except Exception as e:
                        if hasattr(e, 'status_code') and e.status_code == 429:
                            should_continue = False
                            logging.info('*** Rate limited. Error code: {code}, message: {e}'.format(
                                code=str(e.status_code),
                                e=str(e)
                            ))
                        else:
                            logging.info('*** Error executing transaction. Error message: {e}'.format(
                                e=str(e)
                            ))
                            msg = '*'.join([ticker, comment_id, transaction_type, author])
                            logging.info('*** Deleting message from queue: {msg}'.format(msg=msg))
                            self.delete_message_from_queue()
                else:
                    msg = '*'.join([ticker, comment_id, transaction_type, author])
                    logging.info('*** Deleting message from queue: {msg}'.format(msg=msg))
                    self.delete_message_from_queue()
            else:
                should_continue = False
            curr = time.time()

    '''
    gets rowkey for azure table storage, so its inserted in descending order
    '''
    def get_key(self):
        def ticks(dt_utc):
            return (dt_utc - datetime.datetime(1, 1, 1)).total_seconds() * 10000000
        now = int(ticks(datetime.datetime.utcnow()))
        oldest = int(ticks(datetime.datetime(datetime.MAXYEAR, 1, 1)))
        return str(oldest - now)


def main(mytimer2: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer2.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    PS = PurchaseStonks()
    PS.execute()
