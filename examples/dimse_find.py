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
from pydicom.pixel_data_handlers import gdcm_handler, pillow_handler
from pydcmq import consumer_loop, responder_loop, publish_nifti, publish_nifti_study, publish_dcm_series, publish_dcm, reply_dcm, reply_fin

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

def parseCFINDResponses(responses):
    for (status, identifier) in responses:
        if status:
            logging.debug('C-FIND query status: 0x{0:04x}'.format(status.Status))

            # If the status is 'Pending' then identifier is the C-FIND response
            if status.Status in (0xFF00, 0xFF01):
                yield identifier

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
    #responses_copy = []
    count = 0
    for (status, identifier) in responses:
        if status:
            yield (status, identifier)
            #responses_copy.append(deepcopy((status, identifier)))
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
    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            ds3 = deepcopy(ds2)
            ds3.QueryRetrieveLevel = 'SERIES'
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            study_queries.append(ds3)

    series_generators = []
    for query in study_queries:
        series_generators.append(_cFind(query, limit=limit))

    for series_generator in series_generators:
        for (status, identifier) in series_generator:
            if status and status.Status in (0xFF00, 0xFF01):
                yield (status, identifier)

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
    for (status, identifier) in responses:
        if status and status.Status in (0xFF00, 0xFF01):
            ds3 = deepcopy(ds2)
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            ds3.SeriesInstanceUID = identifier.SeriesInstanceUID
            ds3.QueryRetrieveLevel = 'IMAGE'
            series_queries.append(ds3)

    instances_generators = []
    for query in series_queries:
        instances_generators.append(_cFind(query, limit=limit))

    for instance_generator in instances_generators:
        for (status, identifier) in instance_generator:
            if status and status.Status in (0xFF00, 0xFF01):
                yield (status, identifier)
        
def find_studies(ds, limit=0):
    responses = _cFindStudy(ds, limit=limit)
    yield from parseCFINDResponses(responses)

def find_series(ds, limit=0):
    responses = _cFindSeries(ds, limit=limit)
    yield from parseCFINDResponses(responses)

def find_instances(ds, limit=0):
    responses = _cFindInstances(ds, limit=limit)
    return parseCFINDResponses(responses)

async def dcmhandler(channel, ds, uri, method, reply_to):
    if method == 'find.studies':
        retlist = find_studies(ds)
    elif method == 'find.series':
        retlist = find_series(ds)
    elif method == 'find.instances':
        retlist = find_instances(ds)
    else:
        return
    for ret in retlist:
        await reply_dcm(channel, reply_to, ret, uri="")
    await reply_fin(channel, reply_to)

server_ip = "127.0.0.1"
server_ae = "DCM4CHEE"
server_port = 11112
calling_ae = "PYNETDICOM"

if __name__ == '__main__':
    responder_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dimse",
        methods=[
            'find.*',
        ],
        dcmhandler=dcmhandler
    )