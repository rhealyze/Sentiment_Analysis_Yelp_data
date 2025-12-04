import pyspark
import sys
import os
from openai import OpenAI
# Add /opt/spark (the parent of jobs/) to sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)  # /opt/spark
sys.path.insert(0, PROJECT_ROOT)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.functions import from_json, col, when, udf
from config.config import config
from time import sleep
from openai import OpenAI
...

import traceback

def sentiment_analysis(comment: str) -> str:
    if not comment:
        return "empty"

    try:
        from openai import OpenAI
        client = OpenAI(api_key=config["openai"]["api_key"])

        response = client.chat.completions.create(
            model="gpt-4o-mini",  # or another model you can use
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You classify customer review comments into exactly one of: "
                        "POSITIVE, NEGATIVE, NEUTRAL. "
                        "Reply with only one of these words."
                    ),
                },
                {"role": "user", "content": comment},
            ],
            temperature=0,
        )

        label = response.choices[0].message.content.strip().upper()

        return label

    except Exception as e:
        # 🔍 TEMPORARY: log full traceback so we can see the real issue
        print("OpenAI error for comment:", file=sys.stderr)
        traceback.print_exc()
        return f"ERROR: {e.__class__.__name__}"


def start_streaming(spark):
    topic = 'customer_service_review'
    try:
            stream_df = (spark.readStream.format("socket")
                        .option("host", "0.0.0.0")
                        .option("port",9999)
                        .load()
                        )
            schema = StructType([
                 StructField("review_id", StringType()),
                 StructField("user_id", StringType()),
                 StructField("business_id", StringType()),
                 StructField("stars", FloatType()),
                 StructField("date", StringType()),
                 StructField("text", StringType()),
            ])
            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

            sentiment_analysis_udf  = udf(sentiment_analysis, StringType())                      
            stream_df = stream_df.withColumn("feedback",
                                             when(col('text').isNotNull(),sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             )

            kafka_df = stream_df.selectExpr("CAST(review_id as STRING) AS key", "to_json(struct(*)) As value")

            query =(kafka_df.writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                    .option("kafka.security.protocol", config['kafka']['security.protocol'])
                    .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
                    .option('kafka.sasl.jaas.config',
                           'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                           'password="{password}";'.format(
                               username=config['kafka']['sasl.username'],
                               password=config['kafka']['sasl.password']
                           ))
                   .option('checkpointLocation', '/tmp/checkpoint')
                   .option('topic', topic)
                   .start()
                   .awaitTermination())
            #query = stream_df.writeStream.outputMode("append").format("console").options(truncate =False).start()
            #query.awaitTermination()
    except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)
if __name__ == "__main__":
    spark_conn =SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn)