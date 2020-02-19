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
import urllib
import asyncio
import logging
import socket
import random
import sys
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import *

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


class SCUfind(object):
    _instance = None
    ae = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(SCUfind, cls).__new__(cls)
            inst = cls._instance
            # Initialise the Application Entity
            inst.ae = AE(ae_title=calling_ae)
            inst.ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
        return cls._instance

    def __del__(self):
        pass
            

async def _cFind(ds, limit = 0):
    scu = SCUfind()
    ae = scu.ae
    if 'PatientName' in ds and not str(ds.PatientName).endswith('*'):
        ds.PatientName = str(ds.PatientName) + '*'
    # Add a requested presentation context
    sop_class = StudyRootQueryRetrieveInformationModelFind
    assoc = ae.associate(
        server_ip, server_port, ae_title=server_ae
    )
    if not assoc.is_established:
        print('association to dimse server failed')
        return
    
    msg_id = 1
    context = assoc._get_valid_context(sop_class, '', 'scu')
    # Use the C-FIND service to send the identifier
    responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind, msg_id=msg_id)
    count = 0
    for (status, identifier) in responses:
        if status:
            if status.Status in (0xFF00, 0xFF01):
                yield identifier
                count += 1
            if count == limit:
                assoc.send_c_cancel(msg_id, context.context_id)
                break
    assoc.release()

async def _cFindStudy(channel, ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    ds2.QueryRetrieveLevel = 'STUDY'
    async for res in _cFind(ds2, limit = limit):
        await publish(channel, "found.study", res)

async def _cFindSeries(channel, ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    if 'SeriesInstanceUID' not in ds:
        ds2.SeriesInstanceUID = ''
    if ds2.StudyInstanceUID != '':
        ds2.QueryRetrieveLevel = 'SERIES'
        async for res in _cFind(ds2, limit = limit):
            await publish(channel, "found.series", res)
        await publish(channel, "found.study.series", ds2)
        return
    ds2.QueryRetrieveLevel = 'STUDY'
    responses = _cFind(ds2)
    async for identifier in responses:
        ds3 = deepcopy(ds2)
        ds3.QueryRetrieveLevel = 'SERIES'
        ds3.StudyInstanceUID = identifier.StudyInstanceUID
        async for res in _cFind(ds3, limit=limit):
            await publish(channel, "found.series", res)
        await publish(channel, "found.study.series", identifier)

async def _cFindInstances(channel, ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    if 'SeriesInstanceUID' not in ds:
        ds2.SeriesInstanceUID = ''
    if 'SOPInstanceUID' not in ds:
        ds2.SOPInstanceUID = ''
    if ds2.SeriesInstanceUID != '':
        ds2.QueryRetrieveLevel = 'IMAGE'
        for res in await _cFind(ds2, limit = limit):
            await publish(channel, "found.instance", res)
        await publish(channel, "found.series.instances", ds2)
        return
    if ds2.StudyInstanceUID != '':
        ds2.QueryRetrieveLevel = 'SERIES'
        responses = _cFind(channel, ds2)
        async for identifier in responses:
            ds3 = deepcopy(ds2)
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            ds3.SeriesInstanceUID = identifier.SeriesInstanceUID
            ds3.QueryRetrieveLevel = 'IMAGE'
            async for res in _cFind(ds3, limit=limit):
                await publish(channel, "found.instance", res)
            await publish(channel, "found.series.instances", identifier)
        await publish(channel, "found.study.instances", ds2)
        return
    ds2.QueryRetrieveLevel = 'STUDY'
    responses = _cFind(ds2)
    async for identifier in responses:
        ds3 = deepcopy(ds2)
        ds3.QueryRetrieveLevel = 'SERIES'
        ds3.StudyInstanceUID = identifier.StudyInstanceUID
        async for res in _cFind(ds3, limit=limit):
            ds4 = deepcopy(ds4)
            ds4.StudyInstanceUID = res.StudyInstanceUID
            ds4.SeriesInstanceUID = res.SeriesInstanceUID
            ds4.QueryRetrieveLevel = 'IMAGE'
            async for res4 in _cFind(ds4, limit=limit):
                await publish(channel, "found.instance", res4)
            await publish(channel, "found.series.instances", res)
        await publish(channel, "found.study.instances", identifier)
        
async def dcmhandler(channel, ds, uri, method):
    if method == 'find.studies':
        await _cFindStudy(channel, ds, limit=0)
    elif method == 'find.series':
        await _cFindSeries(channel, ds, limit=0)
    elif method == 'find.instances':
        await _cFindInstances(channel, ds, limit=0)

server_ip = "10.3.21.20"
server_ae = "RADWIPACS"
server_port = 11112
calling_ae = "RWSN225M"

if __name__ == '__main__':
    if len(sys.argv) > 1:
        server_ip, server_ae, server_port, calling_ae = sys.argv[1:]
        server_port = int(server_port)
    responder_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            'find.#'
        ],
        dcmhandler=dcmhandler
    )