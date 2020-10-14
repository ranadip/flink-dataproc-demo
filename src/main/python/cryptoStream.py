# Based on https://min-api.cryptocompare.com/documentation/websockets?key=Channels&cat=Trade&api_key=63d4d30c6c92db96f7a89950e16ceb69cefebe0a186397b38998e3e317b783f6

import asyncio
import json
import websockets
from kafka import KafkaProducer

kafka_broker_list='23.236.61.27:9092'
kafka_topic='crypto-trade'
producer = KafkaProducer(bootstrap_servers=kafka_broker_list)
producer.send('sample', b'Hello, World!')
producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')

async def cryptocompare():
    # this is where you paste your api key
    api_key = "63d4d30c6c92db96f7a89950e16ceb69cefebe0a186397b38998e3e317b783f6"
    url = "wss://streamer.cryptocompare.com/v2?api_key=" + api_key
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps({
            "action": "SubAdd",
            "subs": ["0~Coinbase~BTC~USD"],
        }))
        while True:
            try:
                data = await websocket.recv()
            except websockets.ConnectionClosed:
                break
            try:
                producer.send(kafka_topic, data)
                
                data = json.loads(data)
                print(json.dumps(data, indent=4))
            except ValueError:
                print(data)

asyncio.get_event_loop().run_until_complete(cryptocompare())