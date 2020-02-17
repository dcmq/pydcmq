import asyncio
from aio_pika import IncomingMessage, Message, ExchangeType, connect_robust
from .util import datasetFromBinary, datasetToBinary

async def reply_dcm(channel, reply_to, ds, uri, data=None):
    if reply_to == None:
        return
    if data == None:
        data = datasetToBinary(ds)
    await channel.default_exchange.publish(
        Message(
            body=data,
            headers={"uri": uri}
        ),
        routing_key=reply_to
    )
    print(f"dcmq: replied dicom instance {uri} to {reply_to}")

async def reply_fin(channel, reply_to):
    if reply_to == None:
        return
    await channel.default_exchange.publish(
        Message(
            body=b'FIN',
        ),
        routing_key=reply_to
    )
    print(f"dcmq: replied FIN to {reply_to}")

async def reply_start(channel, reply_to):
    if reply_to == None:
        return
    await channel.default_exchange.publish(
        Message(
            body=b'START',
        ),
        routing_key=reply_to
    )
    print(f"dcmq: replied START to {reply_to}")

async def async_responder(server, queue, methods, dcmhandler):
    loop = asyncio.get_running_loop()
    connection = await connect_robust(server, loop=loop)
    print(f"dcmq: connected to {server}")
    channel = await connection.channel()
    dicom_exchange = await channel.declare_exchange(
        'amq.topic', ExchangeType.TOPIC, durable=True
    )
    queue = await channel.declare_queue(queue)
    for method in methods:
        await queue.bind(dicom_exchange, routing_key=method)
        print(f"dcmq: bound queue {queue} to method {method} at topic exchange {dicom_exchange}")


    async def handle_msg(msg: IncomingMessage):
        print(f"dcmq: got message with routing key {msg.routing_key}, wants reply to {msg.reply_to}")
        ds = datasetFromBinary(msg.body)
        uri = ""
        if "uri" in msg.headers:
            uri = msg.headers["uri"]
        if ds != None:
            try:
                await dcmhandler(channel, ds, uri, msg.routing_key, msg.reply_to)
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