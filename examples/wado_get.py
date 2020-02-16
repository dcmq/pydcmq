from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
from pynetdicom import (
    AE, evt, build_role, debug_logger, 
    StoragePresentationContexts,
    VerificationPresentationContexts,
    PYNETDICOM_IMPLEMENTATION_UID,
    PYNETDICOM_IMPLEMENTATION_VERSION
)
from pydicom.uid import (
    ExplicitVRLittleEndian, ImplicitVRLittleEndian,
    ExplicitVRBigEndian, DeflatedExplicitVRLittleEndian, 
    JPEGLossless
)
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelGet,
    StudyRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelMove,
    PatientRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelGet,
    VerificationSOPClass,
    CTImageStorage,
    MRImageStorage,
    BasicTextSRStorage,
    EnhancedSRStorage,
    SegmentationStorage
)
import pydicom
from pydicom import Dataset
from pydicom.tag import Tag
from pydcmq.util import datasetToJSON, datasetFromBinary, datasetToBinary, datasetFromJSON, writeFile, filterBinary, getFilename
from copy import deepcopy
import aiohttp 
import urllib
import asyncio
import logging
import socket
import random
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import consumer_loop, responder_loop, publish_nifti, publish_nifti_study, publish_dcm_series, publish_dcm, publish_dcm_study,reply_dcm, publish_find_instance

logger = logging.getLogger('pynetdicom')
logger.setLevel(logging.INFO)

storageclasses = [MRImageStorage, BasicTextSRStorage, CTImageStorage, EnhancedSRStorage]

transfer_syntax_full = [
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    DeflatedExplicitVRLittleEndian,
    ExplicitVRBigEndian,
    JPEGLossless
]

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
    await writeFile(filepath, ds, data=filedata)
    await publish_dcm(channel, newds, filepath, data=smalldata)
    await reply_dcm(channel, reply, ds, filepath, data=smalldata)
                
                
async def get(channel, ds, reply):
    tasks = []
    current_task = 0
    queue = await channel.declare_queue(exclusive=True)
    await publish_find_instance(channel, ds, queue.name)
    instances = []
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                if message.body == b'FIN':
                    break
                else:
                    newds = datasetFromBinary(message.body)
                    if newds != None:
                        instances += [newds]
    for instance in instances:
        tasks.append(asyncio.create_task(get_instance(channel, instance, reply)))
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
        path = getFilename(ds)
        await publish_dcm_study(channel, ds, path)
    elif method == 'get.series':
        await get(channel, ds, reply_to)
        path = getFilename(ds)
        await publish_dcm_series(channel, ds, path)
    elif method == 'get.instance':
        await get_instance(channel, ds, reply_to)
    else:
        return

endpoint = "http://127.0.0.1:8080/dcm4chee-arc/aets/DCM4CHEE/wado"
MAXTASKS = 50

if __name__ == '__main__':
    responder_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dimse",
        methods=[
            'get.*'
        ],
        dcmhandler=dcmhandler
    )