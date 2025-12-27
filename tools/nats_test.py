import asyncio
import json
from nats.aio.client import Client as NATS

async def publish_test():
    nc = NATS()
    await nc.connect("nats://127.0.0.1:4222")

    data = {
        "type": "external_update",
        "code": "USD",
        "name": "Доллар США",
        "rate": 123.45,
        "time": "12:00:00"
    }

    await nc.publish("currency.updates", json.dumps(data).encode())
    print("Message published to NATS")
    await nc.close()

asyncio.run(publish_test())
