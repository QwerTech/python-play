import json
import re
import sys
from math import ceil
from multiprocessing import Process, Manager, RLock

from kafka import KafkaConsumer, TopicPartition

# To consume latest messages and auto-commit offsets
from config import bootstrap_servers, urlPattern, topic


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


def handleRecords(partitions, counts):
  consumer = createConsumer()
  consumer.assign(list(map(lambda p: TopicPartition(topic, p), partitions)))
  print(f'Started consumer for {topic}:{partitions}')
  for message in consumer:
    handleRecord(message, counts)


def handleRecord(message, counts):
  if message.value['method'] != 'GET': return
  search = re.search(urlPattern, message.value['url'])
  if search:
    contractId = search[1]
    with(lock):
      counts[contractId] = (counts.get(contractId) or 0) + 1


globalConsumer = createConsumer()
lock = RLock()

if __name__ == '__main__':
  manager = Manager()
  counts = manager.dict()
  pool = []
  # for i in globalConsumer.partitions_for_topic(topic):
  processesCount = 10
  partitionsPerProcess = ceil(
      len(globalConsumer.partitions_for_topic(topic)) / processesCount)
  for i in range(processesCount):
    partitions = range(i * partitionsPerProcess, (i + 1) * partitionsPerProcess)
    p = Process(target=handleRecords, args=(partitions, counts,))
    p.start()
    pool.append(p)

  for p in pool:
    p.join()
  print({k: v for k, v in
         sorted(counts.items(), reverse=True, key=lambda item: item[1]) if
         v > 10})
  sys.exit()
