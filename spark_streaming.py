import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkConf

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, ArrayType, DateType
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType, LongType, DoubleType, BooleanType
from pyspark.sql.functions import col, count, when, sum, min, max, expr
from pyspark.sql import functions as F
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


conf = SparkConf().set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").set("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog").set("spark.sql.catalog.glue_catalog.warehouse","s3://robin-poc-bucket/rahul/warehouse/").set("spark.sql.catalog.glue_catalog.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog").set("spark.sql.catalog.glue_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# print("Job started")
# df = spark.read.format("iceberg").load("glue_catalog.iceberg_sample_db.hands")
# df.show(2)

# df = df.select("shard_id").withColumnRenamed("shard_id", "age")
# df.createOrReplaceTempView("sample")
# df = spark.sql("""
#     select cast(age as int) as age
#     from sample limit 10
# """)

# print("writing to ES")
# df.write.format('org.elasticsearch.spark.sql').option("path","sample-index1").option('es.nodes', 'https://vpc-testpspvt-2xjjsrwejwnaxv7l3zoavngysy.ap-south-1.es.amazonaws.com').option('es.port', 443).mode("append").option('es.nodes.wan.only', True).option('es.mapping.id', 'age').save()

# print("Writting completed")

def get_schema():
    SCHEMA = StructType([
        StructField("event_data", StructType([
            StructField("community_cards", ArrayType(StructType([
                StructField("card_suit", StringType()),
                StructField("card_value", IntegerType())
            ]))),
            StructField("community_cards_rit", ArrayType(StructType([
                StructField("card_suit", StringType()),
                StructField("card_value", IntegerType())
            ]))),
            StructField("game_end_timestamp", IntegerType()),
            StructField("game_start_timestamp", IntegerType()),
            StructField("hand_id", LongType()),
            StructField("player_details", ArrayType(StructType([
                StructField("cards", ArrayType(StructType([
                    StructField("card_suit", StringType()),
                    StructField("card_value", IntegerType())
                ]))),
                StructField("chip_stack_end", DoubleType()),
                StructField("chip_stack_start", DoubleType()),
                StructField("position", StringType()),
                StructField("profit_loss", DoubleType()),
                StructField("reference_number", StringType()),
                StructField("seat_index", IntegerType()),
                StructField("session_id", StringType()),
                StructField("total_bets", DoubleType()),
                StructField("total_win", DoubleType()),
                StructField("user_id", LongType()),
                StructField("username", StringType()),
                StructField("rake", StructType([
                    StructField("bonus_rake", DoubleType()),
                    StructField("net_rake", DoubleType()),
                    StructField("service_tax", DoubleType()),
                    StructField("total_rake", DoubleType())
                ]))
            ]))),
            StructField("players_in_hand", IntegerType()),
            StructField("rit_game", IntegerType()),
            StructField("round_details", StructType([
                StructField("flop", StructType([
                    StructField("player_action", ArrayType(StructType([
                        StructField("action", StringType()),
                        StructField("amount", DoubleType()),
                        StructField("chip_stack_end", DoubleType()),
                        StructField("chip_stack_start", DoubleType()),
                        StructField("position", StringType()),
                        StructField("timestamp", IntegerType()),
                        StructField("total_pot", DoubleType()),
                        StructField("user_id", IntegerType()),
                        StructField("username", StringType())
                    ]))),
                    StructField("pot_details", ArrayType(DoubleType()))
                ])),
                StructField("preflop", StructType([
                    StructField("player_action", ArrayType(StructType([
                        StructField("action", StringType()),
                        StructField("amount", DoubleType()),
                        StructField("chip_stack_end", DoubleType()),
                        StructField("chip_stack_start", DoubleType()),
                        StructField("position", StringType()),
                        StructField("timestamp", IntegerType()),
                        StructField("total_pot", DoubleType()),
                        StructField("user_id", IntegerType()),
                        StructField("username", StringType())
                    ]))),
                    StructField("pot_details", ArrayType(DoubleType()))
                ])),
                StructField("river", StructType([
                    StructField("player_action", ArrayType(StructType([
                        StructField("action", StringType()),
                        StructField("amount", DoubleType()),
                        StructField("chip_stack_end", DoubleType()),
                        StructField("chip_stack_start", DoubleType()),
                        StructField("position", StringType()),
                        StructField("timestamp", IntegerType()),
                        StructField("total_pot", DoubleType()),
                        StructField("user_id", IntegerType()),
                        StructField("username", StringType())
                    ]))),
                    StructField("pot_details", ArrayType(DoubleType()))
                ])),
                StructField("showdown", StructType([
                    StructField("pot_details", ArrayType(DoubleType())),
                    StructField("pot_details_rit", ArrayType(DoubleType())),
                    StructField("pot_winners", ArrayType(StructType([
                        StructField("pot_amount", DoubleType()),
                        StructField("pot_contestants", ArrayType(IntegerType())),
                        StructField("winner_details", ArrayType(StructType([
                            StructField("amount", DoubleType()),
                            StructField("timestamp", IntegerType()),
                            StructField("user_id", IntegerType()),
                            StructField("win_type", StringType())
                        ])))
                    ]))),
                    StructField("pot_winners_rit", ArrayType(StructType([
                        StructField("pot_amount", DoubleType()),
                        StructField("pot_contestants", ArrayType(IntegerType())),
                        StructField("winner_details", ArrayType(StructType([
                            StructField("amount", DoubleType()),
                            StructField("timestamp", IntegerType()),
                            StructField("user_id", IntegerType()),
                            StructField("win_type", StringType())
                        ])))
                    ])))
                ])),
                StructField("turn", StructType([
                    StructField("player_action", ArrayType(StructType([
                        StructField("action", StringType()),
                        StructField("amount", DoubleType()),
                        StructField("chip_stack_end", DoubleType()),
                        StructField("chip_stack_start", DoubleType()),
                        StructField("position", StringType()),
                        StructField("timestamp", IntegerType()),
                        StructField("total_pot", DoubleType()),
                        StructField("user_id", IntegerType()),
                        StructField("username", StringType())
                    ]))),
                    StructField("pot_details", ArrayType(DoubleType()))
                ]))
            ])),
            StructField("showdown", BooleanType()),
            StructField("table_info", StructType([
                StructField("big_blind", DoubleType()),
                StructField("is_anonymous", BooleanType()),
                StructField("max_buyin", DoubleType()),
                StructField("min_buyin", DoubleType()),
                StructField("mini_game_type_id", StringType()),
                StructField("players_per_table", IntegerType()),
                StructField("small_blind", DoubleType()),
                StructField("table_child_id", IntegerType()),
                StructField("table_id", IntegerType()),
                StructField("table_type", StringType())
            ])),
            StructField("total_pot", DoubleType()),
            StructField("user_stats", ArrayType(StructType([
                StructField("3bet", BooleanType()),
                StructField("3bet_opportunity", BooleanType()),
                StructField("all_in", BooleanType()),
                StructField("allin_opportunity", BooleanType()),
                StructField("flop_seen", BooleanType()),
                StructField("fold_opportunity", BooleanType()),
                StructField("hp", BooleanType()),
                StructField("pfr_count", BooleanType()),
                StructField("pfr_opportunity", BooleanType()),
                StructField("pr", BooleanType()),
                StructField("time_played", IntegerType()),
                StructField("user_id", IntegerType()),
                StructField("user_won", BooleanType()),
                StructField("vpip", BooleanType()),
                StructField("vpip_opportunity", BooleanType()),
                StructField("won_at_showdown", BooleanType()),
                StructField("wtsd", BooleanType()),
                StructField("fold_on_allin_opportunity", BooleanType()),
                StructField("fold", BooleanType()),
                StructField("3bet_fold_opportunity", BooleanType()),
                StructField("steal_opportunity", BooleanType()),
                StructField("steal_attempt", BooleanType()),
                StructField("fold_on_allin", BooleanType()),
                StructField("check_n_raise_opportunity", BooleanType()),
                StructField("cbet_opportunity", BooleanType()),
                StructField("cbet_success", BooleanType()),
                StructField("fold_to_cbet_opportunity", BooleanType()),
                StructField("folded_to_cbet", BooleanType()),
                StructField("folded_to_3bet", BooleanType()),
                StructField("check_n_raise", BooleanType())
            ]))),
            StructField("game_rake", StructType([
                StructField("bonus_rake", DoubleType()),
                StructField("net_rake", DoubleType()),
                StructField("service_tax", DoubleType()),
                StructField("total_rake", DoubleType())
            ]))
        ])),
        StructField("event_name", StringType()),
        StructField("event_source", StringType()),
        StructField("event_timestamp", IntegerType()),
        StructField("event_uuid", StringType()),
        StructField("event_version", StringType())
    ])
    
    return SCHEMA

# source 
s3_path = "s3://robin-poc-bucket/rahul/test_1/hands_json_data/"
# tables 
hands_bronze_data = "glue_catalog.iceberg_sample_db.hands_bronze_data"
hands_gold_data = "glue_catalog.iceberg_sample_db.hands_gold_data"
hands_gold_data_s3_location = "s3://robin-poc-bucket/rahul/iceberg/hands_gold_data"
iceberg_database = "iceberg_sample_db"

# check_point_locations 
bronze_checkpointlocation = "s3://robin-poc-bucket/rahul/test_1/bronze_checkpointlocation/"
gold_checkpointlocation = "s3://robin-poc-bucket/rahul/test_1/gold_checkpointlocation/"
opensearch_checkpointlocation = "s3://robin-poc-bucket/rahul/test_1/opensearch_checkpointlocation/"

# open search index
es_index = "performance_by_stake_v2"
es_nodes = "https://vpc-testpspvt-2xjjsrwejwnaxv7l3zoavngysy.ap-south-1.es.amazonaws.com"

class Bronze():
    
    def __init__(self):
        self.s3_path = s3_path
        self.iceberg_database = iceberg_database
        self.check_point_location = bronze_checkpointlocation

    def readHandsEvents(self):
        schema = get_schema()
        return spark.readStream.format("json").schema(schema).load(s3_path)
                
    def writeToIcebergTable(self, hands_df):
        sbronzeQuery = ( 
            hands_df.writeStream
                    .queryName("bronze-ingestion")
                    .format("iceberg")
                    .option("checkpointLocation", self.check_point_location)
                    .outputMode("append")
                    .toTable(f"{hands_bronze_data}")
        ) 
        return sbronzeQuery

    def process(self):
        raw_data_df = self.readHandsEvents()
        sQuery = self.writeToIcebergTable(raw_data_df)
        return sQuery
        
    
class Gold():
    def __init__(self):
        self.gold_checkpointlocation = gold_checkpointlocation
        self.opensearch_checkpointlocation = opensearch_checkpointlocation
        self.os_index = es_index
        self.os_nodes = es_nodes

    def readBronzedata(self, replay=False, streamStartTimestamp = ""):
        if replay:
            # specify your read time stamp from here
            streamStartTimestamp = ""
            hands_data_df = spark.readStream.format("iceberg").option("stream-from-timestamp", Long.toString(streamStartTimestamp)).load(f"{hands_bronze_data}")  
        else:
            hands_data_df = spark.readStream.format("iceberg").load(f"{hands_bronze_data}")  
        return hands_data_df

        
    def flatternBronzedata(self, bronze_df):
        bronze_df = bronze_df.withColumn("player_details", expr("event_data.player_details"))
        flattern_hands_data_df  = bronze_df.selectExpr(
            "event_data.hand_id as hand_id", 
            "event_data.game_end_timestamp as game_end_timestamp",
            "event_data.game_start_timestamp as game_start_timestamp",
            "event_data.table_info.big_blind as big_blind",
            "case when event_data.table_info.big_blind = 0 then 0 when event_data.table_info.big_blind = 5 then 2 when event_data.table_info.big_blind = 25 then 10 else event_data.table_info.big_blind/2 end as small_blind",
            "event_name", 
            "event_source", 
            "cast(event_timestamp as timestamp) as event_timestamp", 
            "event_uuid", 
            "event_version",
            "explode(player_details) as player_detail", 
            "date(cast(event_timestamp as timestamp)) as record_date",
            "player_detail.user_id as user_id",
            "player_detail.profit_loss as profit_loss",
            "player_detail.position as position",
            "player_detail.total_bets as stake",
            "player_detail.rake.total_rake as total_rake",
            "player_detail.chip_stack_start as chip_stack_start",
            "player_detail.chip_stack_end as chip_stack_end",
            "player_detail.reference_number as session_id",
            "case when player_detail.total_bets + player_detail.profit_loss = 0 then 0 else  player_detail.total_bets + player_detail.profit_loss + player_detail.rake.total_rake end as win",
            """case when (replace(cast(event_data.table_info.mini_game_type_id as string), '"',"")) in ("Holdem") then 1  when (replace(cast(event_data.table_info.mini_game_type_id as string), '"',"")) in ('Omaha') then 2  when (replace(cast(event_data.table_info.mini_game_type_id as string), '"',"")) in ("FiveCardPLO") then 17  when (replace(cast(event_data.table_info.mini_game_type_id as string), '"',"")) in ('SixCardPLO') then 20  when (replace(cast(event_data.table_info.mini_game_type_id as string), '"',"")) in ("SuperHoldem") then 21  else 25 end as minigames_type_id"""
        ).drop("player_detail")

        return flattern_hands_data_df

    def getAggregates(self, flattern_hands_data_df):
        df_performance_by_stake = flattern_hands_data_df.withWatermark("event_timestamp", "1440 minutes") \
            .groupBy(
                    "user_id", "record_date", "session_id", "minigames_type_id", "big_blind", "small_blind"
            ).agg(
                count("*").alias("number_of_hands"),
                expr("cast((max(game_end_timestamp) - min(game_start_timestamp)) as string)").alias("session_duration"),
                min("game_start_timestamp").alias("session_start_time"),
                max("game_start_timestamp").alias("session_end_time"),
                sum(when((col("win") - col("stake")) > 0, 1).otherwise(0)).alias("winning_hands"),
                sum(when((col("win") - col("stake")) < 0, 1).otherwise(0)).alias("losing_hands"),
                sum("profit_loss").alias("win_amount")
            )

        return df_performance_by_stake
        
    def upsert(self, gold_aggregate_df, batch_id):
        
        # user_id, record_date, session_id,minigames_type_id, big_blind, small_blind
        
        # create table in iceberg
        spark.sql(f"""
            CREATE OR REPLACE TABLE {hands_gold_data} (
                  user_id bigint,
                  record_date date,
                  session_id string,
                  minigames_type_id int,
                  big_blind double,
                  small_blind double,
                  number_of_hands bigint,
                  session_duration string,
                  session_start_time int,
                  session_end_time int,
                  winning_hands bigint,
                  losing_hands bigint,
                  win_amount double)
                LOCATION '{hands_gold_data_s3_location}'
                TBLPROPERTIES (
                  'table_type'='iceberg'
            );
        
        """)
        
    
        gold_aggregate_df.createOrReplaceTempView("tmp_hands_agg")
        merge_statement = f"""MERGE INTO {hands_gold_data} t
                USING tmp_hands_agg s
                ON  ( 
                    s.user_id == t.user_id and 
                    s.record_date = t.record_date and 
                    s.session_id = t.session_id and 
                    s.minigames_type_id = t.minigames_type_id and 
                    s.big_blind = t.big_blind and 
                    s.small_blind = t.small_blind
                )
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """
        gold_aggregate_df._jdf.sparkSession().sql(merge_statement) 


    def saveToTable(self, gold_aggregate_df):
        sGoldUpdateQuery = (   gold_aggregate_df.writeStream
                .queryName("gold-update")
                .option("checkpointLocation", f"{self.gold_checkpointlocation}")
                .outputMode("update")
                .foreachBatch(self.upsert)
                .start()
        )
        return sGoldUpdateQuery

    # def saveToOpenSearch(self, aggregate_df):
    #     sGoldUpdateQuery = (
    #         aggregate_df.writeStream.outputMode('append') 
    #             .format("org.elasticsearch.spark.sql") 
    #             .option("path",self.es_index) 
    #             .option("es.nodes", self.es_nodes) 
    #             .option("es.port", 443) 
    #             .option("es.mapping.id", "session_id")
    #             .option("es.write.operation", "upsert")
    #             .option("checkpointLocation", self.opensearch_checkpointlocation) 
    #             .start()
    #             .awaitTermination()
    #     )
    
    def process_batch_query_b(self, gold_aggregate_df, epoch_id):
        # print('Query B: {}'.format(datetime.fromtimestamp(time.time())))
        #df = df.withColumn('QUERY', F.lit('b'))

        schema = hands_gold_data_schema = StructType([
                    StructField("user_id", LongType(), True),
                    StructField("record_date", DateType(), True),
                    StructField("session_id", StringType(), True),
                    StructField("minigames_type_id", IntegerType(), True),
                    StructField("big_blind", DoubleType(), True),
                    StructField("small_blind", DoubleType(), True),
                    StructField("number_of_hands", LongType(), True),
                    StructField("session_duration", StringType(), True),
                    StructField("session_start_time", IntegerType(), True),
                    StructField("session_end_time", IntegerType(), True),
                    StructField("winning_hands", LongType(), True),
                    StructField("losing_hands", LongType(), True),
                    StructField("win_amount", DoubleType(), True)
                ])

        df = gold_aggregate_df.collect()
        #new_df = df.select("event_data.hand_id").alias("hand_id")
        new_df = spark.createDataFrame(df, schema)
        new_df.write.format('org.elasticsearch.spark.sql').option("path",self.os_index).option('es.nodes', self.os_nodes).option('es.port', 443).mode("append").option('es.nodes.wan.only', True).save()
        
    def saveToOpenSearch(self, df):
        df_query = ( df.writeStream
                        .outputMode('update')
                        .option("checkpointLocation", opensearch_checkpointlocation)
                        .foreachBatch(self.process_batch_query_b) 
                        .start()
                )
        return df_query


    def process(self):
        bronze_df = self.readBronzedata()
        flattern_bronze_df = self.flatternBronzedata(bronze_df)
        gold_aggregate_df = self.getAggregates(flattern_bronze_df)
        
        print("Upload to iceberg started")
        sGoldUpdateQuery = self.saveToTable(gold_aggregate_df)
        # sGoldUpdateQuery.awaitTermination()
        print("Upload to ES started")
        sGoldUpdateQueryOS = self.saveToOpenSearch(gold_aggregate_df) 
        sGoldUpdateQueryOS.awaitTermination()

print("Started Bronze processing ")
br = Bronze()
b = br.process()
# b.awaitTermination()
print("Stopped bronze processing ")

print("Started Gold processing ")
gl = Gold()
gl.process()
print("Stopped Gold processing ")


# def process_batch_query_b(df, epoch_id):
#     print('Query B: {}'.format(datetime.fromtimestamp(time.time())))
#     #df = df.withColumn('QUERY', F.lit('b'))

#     new_df = df.collect()
#     #new_df = df.select("event_data.hand_id").alias("hand_id")
#     new_df_ = spark.createDataFrame(new_df, schema_)

#     final_df = new_df_.select("event_data.hand_id").alias("hand_id")
#     final_df.createOrReplaceTempView('df')
#     final_df_ = spark.sql("""select * from df""")
    
#     index_ = 'spark_streaming_poc_index'
#     final_df_.write.format('org.elasticsearch.spark.sql').option("path",index_).option('es.nodes', 'https://vpc-testpspvt-2xjjsrwejwnaxv7l3zoavngysy.ap-south-1.es.amazonaws.com').option('es.port', 443).mode("append").option('es.nodes.wan.only', True).save()
    
# df_query = df \
#     .writeStream \
#     .outputMode('append') \
#     .option("checkpointLocation", "s3://staging-datalake-raw-layer/pokershots/checkpointing/") \
#     .trigger(processingTime='1 seconds') \
#     .foreachBatch(process_batch_query_b) \
#     .start()
# df_query.awaitTermination()




