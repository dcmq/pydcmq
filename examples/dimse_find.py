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
from pydcmq import consumer_loop, responder_loop, publish_nifti, publish_nifti_study, publish_dcm_series, publish_dcm, reply_dcm, reply_fin, reply_start, publish_found_study

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
            

def _cFind(ds, limit = 0):
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
        return 'association to dimse server failed'
    
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

def _cFindStudy(ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    ds2.QueryRetrieveLevel = 'STUDY'
    yield from _cFind(ds2, limit = limit)

def _cFindSeries(ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    if 'SeriesInstanceUID' not in ds:
        ds2.SeriesInstanceUID = ''
    if  ds2.StudyInstanceUID != '':
        ds2.QueryRetrieveLevel = 'SERIES'
        yield from _cFind(ds2, limit = limit)
        return
    ds2.QueryRetrieveLevel = 'STUDY'
    responses = _cFindStudy(ds2)
    study_queries = []
    for identifier in responses:
        ds3 = deepcopy(ds2)
        ds3.QueryRetrieveLevel = 'SERIES'
        ds3.StudyInstanceUID = identifier.StudyInstanceUID
        study_queries.append(ds3)

    for query in study_queries:
        yield from _cFind(query, limit=limit)

def _cFindInstances(ds, limit = 0):
    ds2 = deepcopy(ds)
    if 'StudyInstanceUID' not in ds:
        ds2.StudyInstanceUID = ''
    if 'SeriesInstanceUID' not in ds:
        ds2.SeriesInstanceUID = ''
    if 'SOPInstanceUID' not in ds:
        ds2.SOPInstanceUID = ''
    if ds2.SeriesInstanceUID != '':
        ds2.QueryRetrieveLevel = 'IMAGE'
        yield from _cFind(ds2, limit = limit)
        return
    ds2.QueryRetrieveLevel = 'SERIES'
    responses = _cFindSeries(ds2)
    series_queries = []
    for identifier in responses:
        ds3 = deepcopy(ds2)
        ds3.StudyInstanceUID = identifier.StudyInstanceUID
        ds3.SeriesInstanceUID = identifier.SeriesInstanceUID
        ds3.QueryRetrieveLevel = 'IMAGE'
        series_queries.append(ds3)

    for query in series_queries:
        yield from _cFind(query, limit=limit)
        
async def dcmhandler(channel, ds, uri, method, reply_to):
    await reply_start(channel, reply_to)
    retlist = []
    if method == 'find.studies':
        for ret in _cFindStudy(ds, limit=0):
            await reply_dcm(channel, reply_to, ret, uri="")
            await publish_found_study(channel, ret)
    elif method == 'find.series':
        for ret in _cFindSeries(ds, limit=0):
            await reply_dcm(channel, reply_to, ret, uri="")
    elif method == 'find.instances':
        for ret in _cFindInstances(ds, limit=0):
            await reply_dcm(channel, reply_to, ret, uri="")
    await reply_fin(channel, reply_to)

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
        queue="dimse",
        methods=[
            'find.#'
        ],
        dcmhandler=dcmhandler
    )