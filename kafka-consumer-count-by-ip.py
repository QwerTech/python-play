import json
import re
import sys
from multiprocessing import Process, Manager

from kafka import KafkaConsumer, TopicPartition

# To consume latest messages and auto-commit offsets
from config import bootstrap_servers, topic, urlPattern


def createConsumer():
  return KafkaConsumer(auto_offset_reset='earliest',
                       enable_auto_commit=True,
                       max_poll_records=10000,
                       max_poll_interval_ms=3000,
                       request_timeout_ms=4000,
                       consumer_timeout_ms=5000,
                       value_deserializer=jsonParse,
                       bootstrap_servers=bootstrap_servers)


counts = {}


def jsonParse(message):
  return json.loads(message.decode('utf-8'))


def handleRecords(partition, counts):
  consumer = createConsumer()
  consumer.assign([TopicPartition(topic, partition)])
  print(f'Started consumer for {topic}:{partition}')
  for message in consumer:
    handleRecord(message, counts)


def handleRecord(message, counts):

  if message.value['method'] == 'GET' and re.match(urlPattern,
                                                   message.value['url']):
    # print(
    #   f'Got message with partition={message.partition} offset={message.offset}')
    ip = message.value['headers']['x-forwarded-for'][0].split(", ")[0]
    counts[ip] = (counts.get(ip) or 0) + 1


globalConsumer = createConsumer()

if __name__ == '__main__':
  manager = Manager()
  counts = manager.dict()
  pool = []
  for i in globalConsumer.partitions_for_topic(topic):
    p = Process(target=handleRecords, args=(i, counts,))
    p.start()
    pool.append(p)

  for p in pool:
    p.join()
  print({k: v for k, v in
         sorted(counts.items(), reverse=True, key=lambda item: item[1])})
  sys.exit()
