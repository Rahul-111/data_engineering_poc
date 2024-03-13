import json
import duckdb
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
from opensearchpy.helpers import bulk
import pandas as pd

#1 Get file from dynamodb
#2 query on the file
#3 read preaggregates file if exists and combine with current query result
#4 upload to s3 (result)
#5 upload to opensearch (result)
#6 update the checkpoint

TABLENAME = 'pokershot_checkpoint_and_stats_data_test_v3'
IndexName = 'flag_index'
s3_bucket = "robin-poc-bucket"
s3_key = "rahul/test_1/duck_db/data.parquet"

host = 'vpc-testpspvt-2xjjsrwejwnaxv7l3zoavngysy.ap-south-1.es.amazonaws.com' 
port = 443
region = 'ap-south-1'
index = "performance_by_stake_v2_from_lambda"
service = 'es'

class DynamoUtils:
    def __init__(self, table_name, index_name):
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
        self.table = self.dynamodb.Table(f'{table_name}')
        self.index_name = index_name

    def create_table(self, table_name, index_name):
        table = dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {   'AttributeName': 'record_date', 'KeyType': 'HASH' },
                {   'AttributeName': 's3_file_path','KeyType': 'RANGE' }
            ],
            AttributeDefinitions=[
                {   'AttributeName': 'record_date','AttributeType': 'S'},
                {   'AttributeName': 's3_file_path','AttributeType': 'S'},
                {   'AttributeName': 'flag', 'AttributeType': 'N'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10
            },
            GlobalSecondaryIndexes = [
                { 
                    'IndexName': self.index_name, 
                    'KeySchema' : [{ 'AttributeName': 'flag','KeyType': 'HASH'}],
                    'Projection' : { 'ProjectionType': 'ALL'},
                    'ProvisionedThroughput' : { 'ReadCapacityUnits': 10,'WriteCapacityUnits': 10,},
                }
            ]
        )
        print("Table status:", table.table_status)


    def get_items(self):
        response = self.table.query(
            IndexName = f'{self.index_name}',
            KeyConditionExpression = "flag =:a",
            ExpressionAttributeValues = {
                ":a": 0
            }
        )
        # TODO add status check here
        return response['Items']

    # item = [{'s3_file_path', 'record_date', 'flag'}]
    def update_items(self, items):        
        for item in items:
            # delete the item
            item['flag'] = 1
            resp = self.table.put_item(Item = item)
            print(f"item : {item}, updated : {resp}")

    # item = {'s3_file_path', 'record_date', 'flag'}
    def insert_items(self, item):
        resp = self.table.put_item(Item = item)



def get_query(files_to_process):
    
    QUERY = f"""With cte as ( SELECT  
                event_data.hand_id as hand_id, 
                event_data.game_end_timestamp as game_end_timestamp,
                event_data.game_start_timestamp as game_start_timestamp,
                event_data.table_info.big_blind as big_blind,
                case 
                    when event_data.table_info.big_blind = 0 then 0 
                    when event_data.table_info.big_blind = 5 then 2 
                    when event_data.table_info.big_blind = 25 then 10 
                    else event_data.table_info.big_blind/2 
                end as small_blind,
                replace(event_data.table_info.mini_game_type_id, '"', '') as mini_game_type_id,
                event_name, 
                event_source, 
                event_timestamp,  
                unnest(event_data.player_details) as players,
                event_timestamp as record_date
            from read_parquet({files_to_process}) 
        ),
        flattern_view as (
            select 
                hand_id,
                game_end_timestamp,
                game_start_timestamp,
                big_blind,
                small_blind,
                mini_game_type_id,
                event_name,
                event_source,
                cast(to_timestamp(event_timestamp) as date) as record_date,
                players.user_id,
                players.profit_loss,
                players.position,
                players.total_bets as stake,
                players.rake.total_rake,
                players.chip_stack_start,
                players.chip_stack_end,
                players.reference_number as session_id,
                case 
                    when players.total_bets + players.profit_loss = 0 then 0 
                    else players.total_bets + players.profit_loss + players.rake.total_rake 
                end as win,
                case 
                    when mini_game_type_id in ('Holdem') then 1  
                    when mini_game_type_id in ('Omaha')  then 2  
                    when mini_game_type_id in ('FiveCardPLO') then 17 
                    when mini_game_type_id in ('SixCardPLO') then 20 
                    when mini_game_type_id in ('SuperHoldem') then 21  
                end as minigames_type_id
            from cte ct
        )
        select 
            user_id,
            record_date,
            session_id,
            minigames_type_id,
            big_blind,
            small_blind,
            count(*) as number_of_hands,
            cast((max(game_end_timestamp) - min(game_start_timestamp)) as string) as session_duration,
            min(game_start_timestamp) as session_start_time,
            max(game_start_timestamp) as session_end_time,
            sum(profit_loss) as win_amount,
            cast(sum(case when (win - stake) < 0 then 1 else 0 end) as int) as losing_hands,
            cast(sum(case when (win - stake) > 0 then 1 else 0 end) as int) as winning_hands
        from flattern_view
        group by user_id, record_date, session_id, minigames_type_id, big_blind, small_blind
    """
    return QUERY


def get_files_helper(Items):
    files_to_process = []
    for item in Items:
        s3_file_path = item['s3_file_path']
        files_to_process.append(s3_file_path)

    return files_to_process

# TODO
# def key_exists(bucket, key):
#     s3 = boto3.client("s3")
#     try:
#         s3.head_object(Bucket=bucket, Key=key)
#         print(f"Key: '{key}' found!")
#         return True 
#     except botocore.exceptions.ClientError as e:
#         if e.response["Error"]["Code"] == "404":
#             print(f"Key: '{key}' does not exist!")
#         else:
#             print("Something else went wrong")
#             raise
#     return False

# TODO 

class OpenSearchUtils:
    def __init__(self, host, port , region, index):
        self.host = host 
        self.port = port 
        self.region = region
        self.index = index 
    
    def get_connection(self):
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, self.region)
        
        client = OpenSearch(
            hosts = [{'host': self.host, 'port': self.port}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection,
            pool_maxsize = 20
        )
            
        return client
        
    def create_index(self, client):
        # Create an index with non-default settings.
        
        index_body = {
            'settings': {
                'index': {
                    'number_of_shards': 1,
                    "number_of_replicas": 0
                    }
                },
            "mappings": {
                
                "properties": {
                    "user_id" : {"type": "long"},
                    "record_date" : {"type": "date", "format": "strict_date_optional_time||epoch_second"},
                    "session_id": {"type": "text"},
                    "minigames_type_id": {"type": "integer"},
                    "big_blind": {"type": "double"},
                    "small_blind": {"type": "double"},
                    "number_of_hands": {"type": "long"},
                    "session_duration": {"type": "text"},
                    "session_start_time": {"type": "integer"},
                    "session_end_time": {"type": "integer"},
                    "winning_hands": {"type": "long"},
                    "losing_hands": {"type": "long"},
                    "win_amount": {"type": "double"}
                }
            },
            "aliases": {
                "sample-alias1": {}
            }
        }
        
        response = client.indices.create(self.index, body=index_body)
        print('\nCreating index:')
        print(response)
        
    def get_records(self, client, id):
        resp = client.get(index=self.index, id=id)
        print(resp['_source'])
        
    # object_data = [(1,2,3), (), ()]
    def generate_payload(self, object_data):
        document = []
        for tup in object_data:
            document.append({
                "user_id": int(tup[0]),
                "record_date": tup[1],
                "session_id": tup[2],
                "minigames_type_id": int(tup[3]),
                "big_blind": tup[4],
                "small_blind": tup[5],
                "number_of_hands": tup[6],
                "session_duration": tup[7],
                "session_start_time": int(tup[8]),
                "session_end_time": int(tup[9]),
                "winning_hands": int(tup[10]),
                "losing_hands": int(tup[11]),
                "win_amount": tup[12]
            })
            
        # print(f"total rows : {len(document)}")
        
        bulk_data = []
        for doc_id, doc in enumerate(document):
            bulk_data.append({"index": {"_index": self.index, "_id": str(doc["user_id"])}})
            bulk_data.append(doc)
            # TODO remove this bulk statement
            # break
            
            
        return bulk_data
        
    def upload_records(self, client, object_data):
        
        bulk_data = self.generate_payload(object_data)
        # bulk_data = '{ "index" : { "_index" : "performance_by_stake_v2_from_lambda", "_id" : "4408349" } } \n {"user_id": "4408349","record_date": "2024-01-05","session_id": "44083491704475007378908","minigames_type_id": 1,"big_blind": 25.0,"small_blind": 10.0,"number_of_hands": 12,"session_duration": "732","session_start_time": 1704475616,"session_end_time": 1704476307,"winning_hands": 383,"losing_hands": 8,"win_amount": 4}'
        
        # print(bulk_data)
        
        response = client.bulk(bulk_data)
        # print(response)
        
# def insert_items(dynamodb, table_name):
#     table = dynamodb.Table(f'{table_name}')
#     s3_file_path = 's3://robin-poc-bucket/rahul/sample_data/handsdata_2023-07-26 17-16-08.parquet'
#     flag = 0
#     record_date = '2023-07-26'
    
#     record = {
#         'record_date': record_date,
#         's3_file_path': s3_file_path,
#         'flag': flag
#     }
    
#     res = table.put_item(Item = record)
#     print(f"record inserted : {res}")


def lambda_handler(event, context):
    print("1.Lambda started")

    dy = DynamoUtils(table_name=TABLENAME, index_name=IndexName)

    # 1 Get file from dynamodb
    response = dy.get_items()
    print(f'response : {response}')
    
    files_to_process = get_files_helper(response)
    print(f'1.files_to_process : {files_to_process}')

    # 2 query on the file
    home_directory="/tmp/duckdb/"
    if not os.path.exists(home_directory):
        os.mkdir(home_directory)
    
    set_config = f"SET home_directory='{home_directory}';INSTALL httpfs;LOAD httpfs;"
    duckdb.sql(set_config)

    query = get_query(files_to_process=files_to_process)
    # print(f"QUERY : {query}")
    

    # # 4 upload to s3 (result)
    upload_to_s3_query = f"COPY ({query}) TO 's3://{s3_bucket}/{s3_key}' (FORMAT PARQUET );"
    result = duckdb.sql(upload_to_s3_query)
    
    # object_data = duckdb.sql(query).fetchall()
    object_data = duckdb.sql(query).df()
    print(object_data.head())

    # 5 upload to OpenSearch
    # osu = OpenSearchUtils(host=host, port=port, region=region, index=index)
    # client = osu.get_connection()
    # osu.get_records(client=client, id="J8kz6owB597z23gdFjon")
    # osu.create_index(client)    
    # osu.upload_records(client=client, object_data = object_data)


    # 6 update the checkpoint
    # dy.update_items(items=response)
    # print("6. Checkpoint updated successfully")
    
    print("code commit")
    return {
        'statusCode': 200,
        'body': json.dumps(f'sql')
    }
