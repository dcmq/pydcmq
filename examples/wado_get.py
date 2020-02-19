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

async def get_instance(channel, ds):
    filedata = await _wadoURIDownload(ds, endpoint)
    smalldata, newds = filterBinary(filedata)
    filepath = getFilename(newds)
    await writeFile(filepath, newds, data=filedata)
    await publish(channel, "stored.instance", newds, uri=filepath, data=smalldata)
 
                
async def get(channel, ds):
    await publish(channel, "find.instances", ds)
    

async def dcmhandler(channel, ds, uri, method):
    if method == 'get.study':
        await get(channel, ds)
    elif method == 'get.series':
        await get(channel, ds)
    elif method == 'found.instance':
        await get_instance(channel, ds)
    elif method == "found.series.instances":
        #newds = first image in series
        await publish(channel, "stored.series", newds, uri=uri)
    elif method == "found.study.instances":
        #newds = first image in study
        await publish(channel, "stored.study", newds, uri=uri)


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
            'found.instance',
            'found.series.instances',
            'found.study.instances'
        ],
        dcmhandler=dcmhandler
    )