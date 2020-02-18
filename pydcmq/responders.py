import asyncio
from aio_pika import IncomingMessage, Message, ExchangeType, connect_robust
from .util import datasetFromBinary, datasetToBinary

async def async_responder(server, queue, methods, dcmhandler):
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
        print(f"dcmq: got message with routing key {msg.routing_key}")
        ds = datasetFromBinary(msg.body)
        uri = ""
        if "uri" in msg.headers:
            uri = msg.headers["uri"]
        if ds != None:
            try:
                await dcmhandler(channel, ds, uri, msg.routing_key)
                msg.ack()
            except Exception as e:
                msg.reject(requeue=True)
                raise(e)

    print(f"dcmq: awaiting messages")
    await queue.consume(handle_msg)

def responder_loop(server, queue, methods, dcmhandler, loop = None):
    run_loop = False
    if loop == None:
        loop = asyncio.new_event_loop()
        run_loop = True
    asyncio.get_child_watcher().attach_loop(loop)
    loop.create_task(
        async_responder( 
            server=server,
            queue=queue,
            methods=methods,
            dcmhandler=dcmhandler
        )
    )
    if run_loop:
        loop.run_forever()