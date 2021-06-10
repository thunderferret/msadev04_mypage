from kafka import KafkaConsumer
from json import loads
import time
 
  # topic, broker list 
consumer = KafkaConsumer('topic1', bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest', enable_auto_commit=True, group_id='my-group', consumer_timeout_ms=1000 ) # consumer list를 가져온다
print('[begin] get consumer list') 

while True:
  time.sleep(3)
  for message in consumer:
    print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % ( message.topic, message.partition, message.offset, message.key, message.value ))
    print('[end] get consumer list')
    s1=str(message.value).split(":")
    print(s1)
    if(s1[0]=="reserved"):
      print("reserved"+s1[1:])
    elif(s1[0]=="cancelled"):
      print("cancelled")
    else:
      print(s1)


