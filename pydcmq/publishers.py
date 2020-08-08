import asyncio
import pydicom
from aio_pika import IncomingMessage, Message, ExchangeType, connect_robust
from .util import datasetToBinary, writeFile

async def publish(channel, routing_key, ds, uri="", data=None):
    dicom_exchange = await channel.declare_exchange(
        'amq.topic', ExchangeType.TOPIC, durable=True
    )
    if data == None:
        data = datasetToBinary(ds)
    header = {}
    if uri != "":
        header["uri"] = uri
    await dicom_exchange.publish(
        Message(
            body=data,
            headers=header
        ),
        routing_key=routing_key
    )
    if routing_key == "stored.instance":
        print(f"dcmq: published dicom instance {uri}")
    elif routing_key == "find.instances":
        print(f"dcmq: published find instance request for study {ds.StudyInstanceUID}")
    elif routing_key == "found.study":
        print(f"dcmq: published found.study for study {ds.StudyInstanceUID}")
    elif routing_key == "found.series":
        print(f"dcmq: published found.series for series {ds.SeriesInstanceUID} from study {ds.StudyInstanceUID}")
    elif routing_key == "found.study.series":
        print(f"dcmq: published found.study.series for study {ds.StudyInstanceUID}")
    elif routing_key == "found.instance":
        print(f"dcmq: published found.instance")
    elif routing_key == "stored.series":
        print(f"dcmq: published dicom series {uri}")
    elif routing_key == "stored.study":
        print(f"dcmq: published dicom study {uri}")
    elif routing_key == "stored.series.nii":
        print(f"dcmq: published nifti series {uri}")
    elif routing_key == "stored.study.nii":
        print(f"dcmq: published nifti study {uri}")
    else:
        print(f"dcmq: published {uri} with route {routing_key}")


async def async_publish_study(server, generator):
    loop = asyncio.get_running_loop()
    connection = await connect_robust(server, loop=loop)
    async with connection:
        print(f"dcmq: connected to {server}")
        channel = await connection.channel()
        for (ds, uri) in generator:
            await publish(channel, "stored.instance", ds, uri=str(uri))
        await publish(channel, "stored.study", ds, uri=str(uri.parents[1]))

def publish_study_generator(server, generator):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(
        async_publish_study( 
            server=server,
            generator=generator
        )
    )
    loop.close()