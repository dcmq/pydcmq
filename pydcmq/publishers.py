import asyncio
from aio_pika import IncomingMessage, Message, ExchangeType, connect_robust
from .util import datasetToBinary, writeFile

async def publish_dcm(channel, ds, uri):
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    await dicom_exchange.publish(
        Message(
            body=datasetToBinary(ds),
            headers={"uri": uri}
        ),
        routing_key="stored.instance"
    )
    print(f"dcmq: published dicom instance {uri}")

async def publish_dcm_series(channel, ds, uri):
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    await dicom_exchange.publish(
        Message(
            body=datasetToBinary(ds),
            headers={"uri": uri}
        ),
        routing_key="stored.series"
    )
    print(f"dcmq: published dicom series {uri}")

async def publish_dcm_study(channel, ds, uri):
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    await dicom_exchange.publish(
        Message(
            body=datasetToBinary(ds),
            headers={"uri": uri}
        ),
        routing_key="stored.study"
    )
    print(f"dcmq: published dicom study {uri}")

async def publish_nifti(channel, ds, uri):
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    await dicom_exchange.publish(
        Message(
            body=datasetToBinary(ds),
            headers={"uri": uri}
        ),
        routing_key="stored.series.nii"
    )
    print(f"dcmq: published nifti series {uri}")

async def publish_nifti_study(channel, ds, uri):
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    await dicom_exchange.publish(
        Message(
            body=datasetToBinary(ds),
            headers={"uri": uri}
        ),
        routing_key="stored.study.nii"
    )
    print(f"dcmq: published nifti study {uri}")


async def async_publish(server, filedir):
    loop = asyncio.get_running_loop()
    connection = await connect_robust(server, loop=loop)
    print(f"dcmq: connected to {server}")
    channel = await connection.channel()
    dicom_exchange = await channel.declare_exchange(
        'dicom', ExchangeType.TOPIC
    )
    filepath = await writeFile(ds, data=filedata)
    await dicom_exchange.publish(
        aio_pika.Message(
            body=smalldata,
            headers={"uri": filepath}
        ),
        routing_key="stored.instance"
    )

def publish(server, filedir):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        async_publish( 
            server=server,
            filedir=filedir
        )
    )