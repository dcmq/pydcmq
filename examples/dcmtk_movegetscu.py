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
import sys, os
from os import listdir
from os.path import isfile, join
import logging
import socket
import random
import tempfile
from pathlib import Path
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import consumer_loop, responder_loop, publish_nifti, \
    publish_nifti_study, publish_dcm_series, publish_dcm, \
    publish_dcm_study,reply_dcm, publish_find_instance,\
    reply_fin, reply_start
import shutil
 
def movefile(sourcePath, dst):
    dstDir = os.path.dirname(dst)
    if os.path.isdir(dstDir) == False:
        os.makedirs(dstDir)
    shutil.move(sourcePath, dstDir)

pydicom.config.pixel_data_handlers = [gdcm_handler, pillow_handler]

server_ip = "10.3.21.20"
server_ae = "RADWIPACS"
server_port = 11112
calling_ae = "RWSN225M"
move_dest_port = 4006
dimse_mode = "GET" # "GET" or "MOVE"

async def get(channel, ds, reply):
    with tempfile.TemporaryDirectory() as tmpdirname:
        dcmpath = str(join(tmpdirname, "tmp.dcm"))
        pydicom.dcmwrite(dcmpath, ds)
        print(ds)
        if dimse_mode == "MOVE":
            cmd = f"movescu -v -aet {calling_ae} -aec {server_ae} -S -od {tmpdirname} --port {move_dest_port} {server_ip} {server_port} {dcmpath}"
        else:
            cmd = f"getscu -v -aet {calling_ae} -aec {server_ae} -S -od {tmpdirname} {server_ip} {server_port} {dcmpath}"
        print(cmd)
        os.system(cmd)
        onlyfiles = [f for f in listdir(tmpdirname) if isfile(join(tmpdirname, f))]
        for f in onlyfiles:
            if f == "tmp.dcm":
                continue
            tmpfilepath = str(join(tmpdirname, f))
            newds = pydicom.dcmread(tmpfilepath, stop_before_pixels=True)
            filepath = getFilename(newds)
            movefile(tmpfilepath, filepath)
            await publish_dcm(channel, newds, filepath)
            await reply_dcm(channel, reply, newds, filepath)
    

async def dcmhandler(channel, ds, uri, method, reply_to):
    await reply_start(channel, reply_to)
    queryds = Dataset()
    if method == 'get.instance':
        queryds.QueryRetrieveLevel = 'IMAGE'
        queryds.StudyInstanceUID = ds.StudyInstanceUID
        queryds.SeriesInstanceUID = ds.SeriesInstanceUID
        queryds.SOPInstanceUID = ds.SOPInstanceUID
        await get(channel, queryds, reply_to)
    if method == 'get.study':
        queryds.QueryRetrieveLevel = 'STUDY'
        queryds.StudyInstanceUID = ds.StudyInstanceUID
        await get(channel, queryds, reply_to)
        uri = getFilename(ds)
        await publish_dcm_study(channel, ds, uri)
    if method == 'get.series':
        queryds.QueryRetrieveLevel = 'SERIES'
        queryds.StudyInstanceUID = ds.StudyInstanceUID
        queryds.SeriesInstanceUID = ds.SeriesInstanceUID
        await get(channel, queryds, reply_to)
        uri = getFilename(ds)
        await publish_dcm_series(channel, ds, uri)
    await reply_fin(channel, reply_to)


if __name__ == '__main__':
    responder_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dcmtk_movegetscu",
        methods=[
            'get.*'
        ],
        dcmhandler=dcmhandler
    )