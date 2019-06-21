import boto3
import json
from datetime import datetime
import time

if __name__ == "__main__":
    my_stream_name = my_stream_name = 'face_rekogniztion_result'

    kinesis_client = boto3.client('kinesis', region_name='us-west-2')

    response = kinesis_client.describe_stream(StreamName=my_stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                          ShardId=my_shard_id,
                                                          ShardIteratorType='LATEST')

    my_shard_iterator = shard_iterator['ShardIterator']

    record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                                  Limit=2)

    while 'NextShardIterator' in record_response:
        record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                      Limit=2)

        if len(record_response["Records"]) > 0:

            for rec in record_response["Records"]:


                data_str = rec["Data"]
                data_str = ''.join([chr(p) for p in data_str])

                similarities = data_str.split('"Similarity":')
                similarities.pop(0)

                for s in similarities:
                    similarity = float(s.split(",")[0])
                    face_name = data_str.split('"ExternalImageId":')[1].split("\"")[1]

                    if similarity > 70:
                        print(face_name)

        # wait for 5 seconds
        time.sleep(5)