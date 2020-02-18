import asyncio
from aio_pika import IncomingMessage, ExchangeType, connect_robust
from .util import datasetFromBinary

async def async_consumer(server, queue, methods, dcmhandler):
    loop = asyncio.get_running_loop()
    connection = await connect_robust(server, loop=loop)
    print(f"dcmq: connected to {server}")
    channel = await connection.channel()
    dicom_exchange = await channel.declare_exchange(
        'amq.topic', ExchangeType.TOPIC, durable=True
    )
    if queue == "":
        queue = await channel.declare_queue()
    else:
        queue = await channel.declare_queue(queue)
    for method in methods:
        await queue.bind(dicom_exchange, routing_key=method)
        print(f"dcmq: bound queue {queue} to method {method} at topic exchange {dicom_exchange}")


    async def handle_msg(msg: IncomingMessage):
        if not "uri" in msg.headers:
            uri = ""
        else:
            uri = msg.headers['uri']
        print(f"dcmq: got message with routing key {msg.routing_key} and uri {uri}")
        ds = datasetFromBinary(msg.body)
        if ds != None:
            try:
                await dcmhandler(channel, ds, uri)
                msg.ack()
            except Exception as e:
                msg.reject(requeue=True)
                raise(e)

    print(f"dcmq: awaiting messages")
    await queue.consume(handle_msg)

def consumer_loop(server, queue, methods, dcmhandler):
    loop = asyncio.new_event_loop()
    asyncio.get_child_watcher().attach_loop(loop)
    loop.create_task(
        async_consumer( 
            server=server,
            queue=queue,
            methods=methods,
            dcmhandler=dcmhandler
        )
    )
    loop.run_forever()