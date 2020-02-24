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
from pydcmq import *

logger = logging.getLogger('pynetdicom')
logger.setLevel(logging.INFO)

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

ownIP = get_ip()

ae = None
if ownIP.startswith('10'):
    server_ip = "10.3.21.20"
    server_ae = b"RADWIPACS"
    server_port = 11112
    calling_ae = b"RWSN225M"
    move_dest_port = 4006
    dimse_mode = "C-MOVE"
    use_wado = True
    wado_endpoint = "http://10.3.21.20:8080/wado/wado"
else:
    server_ip = "127.0.0.1" 
    server_ae = b"DCM4CHEE" 
    server_port = 11112 
    calling_ae = b"PYNETDICOM"
    use_wado = True
    wado_endpoint = "http://127.0.0.1:8080/dcm4chee-arc/aets/DCM4CHEE/wado"

# Initialise the Application Entity
if len(sys.argv) > 1:
    server_ip, server_ae, server_port, calling_ae, wado_endpoint = sys.argv[1:]
    server_port = int(server_port)
    server_ae = server_ae.encode()
ae = AE(ae_title=calling_ae)
ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

def _wadoURIURL(ds):
    payload = {'requestType': 'WADO', 
        'studyUID': ds.StudyInstanceUID,
        'seriesUID': ds.SeriesInstanceUID,
        'objectUID': ds.SOPInstanceUID, 
        'contentType': 'application/dicom'}
    return wado_endpoint + '?' + urllib.parse.urlencode(payload)

def _dimseURI():
    return f"dimse://{dimse_mode}:{calling_ae}:{move_dest_port}@{server_ae}:{server_ip}:{server_port}"
        

async def _cFind(ds, limit = 0):
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
    print(ds)
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
        count = 0
        for res in await _cFind(ds2, limit = limit):
            if use_wado: uri = _wadoURIURL(res,)
            else: uri=_dimseURI()
            await publish(channel, "found.instance", res, uri=uri)
            count += 1
        if count>0: await publish(channel, "found.series.instances", ds2)
        return
    if ds2.StudyInstanceUID != '':
        ds2.QueryRetrieveLevel = 'SERIES'
        responses = _cFind(ds2)
        count0 = 0
        async for identifier in responses:
            ds3 = deepcopy(ds2)
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            ds3.SeriesInstanceUID = identifier.SeriesInstanceUID
            ds3.QueryRetrieveLevel = 'IMAGE'
            async for res in _cFind(ds3, limit=limit):
                if use_wado: uri = _wadoURIURL(res)
                else: uri=_dimseURI()
                await publish(channel, "found.instance", res, uri=uri)
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
            ds4 = deepcopy(ds3)
            ds4.SeriesInstanceUID = res.SeriesInstanceUID
            ds4.QueryRetrieveLevel = 'IMAGE'
            async for res4 in _cFind(ds4, limit=limit):
                if use_wado: uri = _wadoURIURL(res4)
                else: uri=_dimseURI()
                await publish(channel, "found.instance", res4, uri=uri)
            await publish(channel, "found.series.instances", res)
        await publish(channel, "found.study.instances", identifier)
        
async def dcmhandler(channel, ds, uri, method):
    if method == 'find.studies':
        await _cFindStudy(channel, ds, limit=0)
    elif method == 'find.series':
        await _cFindSeries(channel, ds, limit=0)
    elif method == 'find.instances':
        await _cFindInstances(channel, ds, limit=0)


if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            'find.#'
        ],
        dcmhandler=dcmhandler
    )