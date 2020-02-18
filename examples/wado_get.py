from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
from pydicom.uid import (
    ExplicitVRLittleEndian, ImplicitVRLittleEndian,
    ExplicitVRBigEndian, DeflatedExplicitVRLittleEndian, 
    JPEGLossless
)
import pydicom
from pydicom import Dataset
from pydicom.tag import Tag
from pydcmq.util import datasetToJSON, datasetFromBinary, datasetToBinary, datasetFromJSON, writeFile, filterBinary, getFilename
from copy import deepcopy
import aiohttp 
import urllib
import asyncio
import sys
import logging
import socket
import random
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import *
pydicom.config.pixel_data_handlers = [gdcm_handler, pillow_handler]


def _wadoURIURL(ds, endpoint):
    payload = {'requestType': 'WADO', 
        'studyUID': ds.StudyInstanceUID,
        'seriesUID': ds.SeriesInstanceUID,
        'objectUID': ds.SOPInstanceUID, 
        'contentType': 'application/dicom'}
    return endpoint + '?' + urllib.parse.urlencode(payload)


async def _wadoURIDownload(ds, endpoint):
    url = _wadoURIURL(ds, endpoint)
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()
    return data

async def get_instance(channel, ds, reply):
    filedata = await _wadoURIDownload(ds, endpoint)
    smalldata, newds = filterBinary(filedata)
    filepath = getFilename(newds)
    await writeFile(filepath, newds, data=filedata)
    await publish_dcm(channel, newds, filepath, data=smalldata)
    await reply_dcm(channel, reply, newds, filepath, data=smalldata)
                
                
async def get(channel, ds, reply):
    tasks = []
    current_task = 0
    queue = await channel.declare_queue(exclusive=True)
    await publish_find_instance(channel, ds, queue.name)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                if message.body == b'FIN':
                    break
                newds = datasetFromBinary(message.body)
                if newds == None:
                    continue
                tasks.append(asyncio.create_task(get_instance(channel, newds, reply)))
                # if too much open tasks, await them
                if len(tasks) - current_task > MAXTASKS:
                    while current_task < len(tasks):
                        await tasks[current_task]
                        current_task += 1
    for task in tasks[current_task:]:
        await task
    

async def dcmhandler(channel, ds, uri, method, reply_to):
    if method == 'get.study':
        await get(channel, ds, reply_to)
        uri = getFilename(ds)
        await publish_dcm_study(channel, ds, uri)
    elif method == 'get.series':
        await get(channel, ds, reply_to)
        uri = getFilename(ds)
        await publish_dcm_series(channel, ds, uri)
    elif method in ['get.instance',]:
        await get_instance(channel, ds, reply_to)
    await reply_fin(channel, reply_to)

endpoint = "http://127.0.0.1:8080/dcm4chee-arc/aets/DCM4CHEE/wado"
endpoint = "http://10.3.21.20:8080/wado/wado"
MAXTASKS = 50

if __name__ == '__main__':
    if len(sys.argv) > 1:
        endpoint = sys.argv[1]
    responder_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            'get.*',
        ],
        dcmhandler=dcmhandler
    )