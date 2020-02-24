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
import sys,os
import logging
import socket
import random
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import *
pydicom.config.pixel_data_handlers = [gdcm_handler, pillow_handler]

async def _URIDownload(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.read()
    return data

async def get_instance(channel, ds, uri):
    filedata = await _URIDownload(uri)
    smalldata, newds = filterBinary(filedata)
    filepath = getFilename(newds)
    await writeFile(filepath, newds, data=filedata)
    #await publish(channel, "stored.instance", newds, uri=filepath, data=smalldata)
 
                
async def get(channel, ds):
    await publish(channel, "find.instances", ds)
    

async def dcmhandler(channel, ds, uri, method):
    if method == 'get.study':
        await get(channel, ds)
    elif method == 'get.series':
        await get(channel, ds)
    elif method == 'found.instance':
        if uri.startswith("http"):
            await get_instance(channel, ds, uri)
    elif method == "found.series.instances":
        #newds = first image in series
        path = getFilename(ds)
        for i in range(2):
            for root, dirs, files in os.walk(path):
                for name in files:
                    try:
                        newds = pydicom.dcmread(os.path.join(root, name), stop_before_pixels=True)
                    except Exception as e:
                        print(e)
                        continue
                    await publish(channel, "stored.series", newds, uri=path)
                    return
            #couldnt find a readable file, wait a second and try again
            if i == 0: await asyncio.sleep(1)

    elif method == "found.study.instances":
        #newds = first image in study
        path = getFilename(ds)
        for i in range(2):
            for root, dirs, files in os.walk(path):
                for name in files:
                    newds = pydicom.dcmread(os.path.join(root, name), stop_before_pixels=True)
                    await publish(channel, "stored.study", newds, uri=path)
                    return
            #couldnt find a readable file, wait a second and try again
            if i == 0: await asyncio.sleep(1)

MAXTASKS = 50

if __name__ == '__main__':
    subscriber_loop(
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