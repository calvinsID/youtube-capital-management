import datetime
import logging

import azure.functions as func

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
        developer_key = os.getenv("YOUTUBE_API_KEY_2")

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


class YoutubeComments(MainClass):
    def __init__(self):
        super().__init__()

        self.video_id = os.environ["VIDEO_ID_2"]
        self.key_word = "heycalvinc:"
        self.allowed_transaction_types = ["buy", "sell"]

    '''
    check if comment_id is already in transactions table.
    if it is, should not continue
    '''
    def already_processed_before(self, comment_id):
        comments = self.table_storage_client.query_entities(
            'transactions', filter="RowKey eq '{comment_id}'".format(comment_id=comment_id))
        
        alreadyProcessed = False
        for comment in comments:
            alreadyProcessed = True
        return alreadyProcessed

    '''
    get pagetoken from db
    '''
    def get_pagetoken(self):
        tokens = self.table_storage_client.query_entities(
            'pagetoken', filter=None, num_results=1)
        token = None
        for t in tokens:
            token = t['value']
        logging.info('$$$ Got pagetoken: {num}'.format(num=str(token)))
        return token

    '''
    update pagetoken in db
    '''
    def update_pagetoken(self, token):
        t = {
            'PartitionKey': "1",
            "RowKey": "1",
            'value': token
        }
        self.table_storage_client.update_entity('pagetoken', t)
        logging.info('$$$ Updated pagetoken: {num}'.format(num=str(token)))

    '''
    determine if its a valid ticker
    '''
    def is_valid_asset(self, ticker):
        try:
            asset = self.alpaca_api_client.get_asset(ticker)
            if asset.tradable and asset.status == 'active':
                return True
        except Exception as e:
            return False
        return False

    '''
    check if comment text is formatted correctly (ex. calvin: buy SPY)
    if correct:
        insert to queue, insert to transactions table
    '''
    def process_comment(self, comment_id, comment_text, author, author_dp, author_channel):
        ticker = None
        transaction_type = None
        formatted_correctly = False
        comment_text = comment_text.strip()
        split_comment = comment_text.split(' ')

        if len(split_comment) == 3:
            if split_comment[0] == self.key_word and split_comment[1].lower() in self.allowed_transaction_types:
                formatted_correctly = True
                transaction_type = split_comment[1].lower()
                ticker = split_comment[2].upper()

        if formatted_correctly:
            logging.info('$$ Processing comment with id: {comment_id}'.format(comment_id=comment_id))
            commentEntity = {
                'PartitionKey': ticker,
                'RowKey': comment_id,
                'TransactionType': transaction_type,
                'Author': author,
                'AuthorDisplayPic': author_dp,
                'AuthorChannelURL': author_channel
            }
            self.table_storage_client.insert_entity('transactions', commentEntity)

            message = "{ticker}*{comment_id}*{transaction_type}*{author}".format(ticker=ticker, comment_id=comment_id, transaction_type=transaction_type, author=author)
            self.transaction_queue_client.send_message(message)
        else:
            logging.info('$$ Processing comment failed: {comment_text}'.format(comment_text=comment_text))

    '''
    keep polling youtube comments until rate limited
        check if comment is the last seen one
        if not and doesn't already exist:
            enter buy/sells into transaction queue
    youtube rate limit is 10,000 units/day
    '''
    def pull_youtube_comments(self) -> None:
        try:
            should_continue = True
            nextPageToken = self.get_pagetoken()

            while should_continue:
                request = self.youtube_client.liveChatMessages().list(
                    part="snippet,authorDetails",
                    maxResults=2000,
                    liveChatId=self.video_id,
                    pageToken=nextPageToken
                )
                response = request.execute()
                nextPageToken = response['nextPageToken'] if 'nextPageToken' in response else None
                
                if nextPageToken:
                    self.update_pagetoken(nextPageToken)
                else:
                    should_continue = False

                numberOfResults = response['pageInfo']['totalResults']
                resultsPerPage = response['pageInfo']['resultsPerPage']
                if numberOfResults == resultsPerPage:
                    should_continue = False

                logging.info('$$$ Number of comments to process: {num}'.format(num=str(numberOfResults)))

                for comment in response['items']:
                    comment_id = comment['id']
                    if not self.already_processed_before(comment_id):
                        try:
                            comment_text = comment['snippet']['displayMessage']
                            comment_author_name = comment['authorDetails']['displayName']
                            comment_author_dp = comment['authorDetails']['profileImageUrl']
                            comment_author_channel_url = comment['authorDetails']['channelUrl']

                            self.process_comment(comment_id=comment_id, comment_text=comment_text, author=comment_author_name, author_dp=comment_author_dp, author_channel=comment_author_channel_url)
                        except Exception as e:
                            logging.exception('$$$ Exception processing comment. Error: {err}'.format(err=e))    
                    else:
                        try:
                            comment_text = comment['snippet']['displayMessage']
                            logging.info('$$ Already processed: {comment_text}'.format(comment_text=comment_text))
                        except Exception as e:
                            logging.exception('$$$ Exception processing comment. Error: {err}'.format(err=e))  
        except Exception as e:
            logging.exception('$$$ Exception pulling youtube comments. Error: {err}'.format(err=e))


def main(mytimer3: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer3.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    YC = YoutubeComments()
    YC.pull_youtube_comments()
