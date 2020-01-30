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