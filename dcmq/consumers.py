import asyncio
from aio_pika import IncomingMessage, ExchangeType, connect_robust
from .util import datasetFromBinary

async def async_consumer(loop, server, methods, dcmhandler):
    connection = await connect_robust(server, loop=loop)
    channel = await connection.channel()
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    queue = await channel.declare_queue()
    for method in methods:
        await queue.bind(dicom_exchange, routing_key=method)


    async def handle_msg(msg: IncomingMessage):
        print(msg)
        with msg.process():
            ds = datasetFromBinary(msg.body)
            if ds != None:
                dcmhandler(ds)

    await queue.consume(handle_msg)