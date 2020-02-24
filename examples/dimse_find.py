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


class SCUfind(object):
    _instance = None
    ae = None
    server_ip = "10.3.21.20"
    server_ae = b"RADWIPACS"
    server_port = 11112
    calling_ae = b"RWSN225M"
    move_dest_port = 4006
    dimse_mode = "C-MOVE"
    use_wado = True
    wado_endpoint = "http://10.3.21.20:8080/wado/wado"

    server_ip = "127.0.0.1" 
    server_ae = b"DCM4CHEE" 
    server_port = 11112 
    calling_ae = b"PYNETDICOM"
    use_wado = True
    wado_endpoint = "http://127.0.0.1:8080/dcm4chee-arc/aets/DCM4CHEE/wado"

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(SCUfind, cls).__new__(cls)
            inst = cls._instance
            # Initialise the Application Entity

            if len(sys.argv) > 1:
                inst.server_ip, inst.server_ae, inst.server_port, inst.calling_ae, inst.wado_endpoint = sys.argv[1:]
                inst.server_port = int(inst.server_port)
                inst.server_ae = inst.server_ae.encode()
            inst.ae = AE(ae_title=inst.calling_ae)
            inst.ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
        return cls._instance

    def __del__(self):
        pass

    def _wadoURIURL(self, ds):
        payload = {'requestType': 'WADO', 
            'studyUID': ds.StudyInstanceUID,
            'seriesUID': ds.SeriesInstanceUID,
            'objectUID': ds.SOPInstanceUID, 
            'contentType': 'application/dicom'}
        return self.wado_endpoint + '?' + urllib.parse.urlencode(payload)

    def _dimseURI(self):
        return f"dimse://{self.dimse_mode}:{self.calling_ae}:{self.move_dest_port}@{self.server_ae}:{self.server_ip}:{self.server_port}"
            

    async def _cFind(self, ds, limit = 0):

        scu = SCUfind()
        ae = scu.ae
        if 'PatientName' in ds and not str(ds.PatientName).endswith('*'):
            ds.PatientName = str(ds.PatientName) + '*'
        # Add a requested presentation context
        sop_class = StudyRootQueryRetrieveInformationModelFind
        assoc = ae.associate(
            self.server_ip, self.server_port, ae_title=self.server_ae
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

    async def _cFindStudy(self, channel, ds, limit = 0):
        ds2 = deepcopy(ds)
        print(ds)
        if 'StudyInstanceUID' not in ds:
            ds2.StudyInstanceUID = ''
        ds2.QueryRetrieveLevel = 'STUDY'
        async for res in self._cFind(ds2, limit = limit):
            await publish(channel, "found.study", res)

    async def _cFindSeries(self, channel, ds, limit = 0):
        ds2 = deepcopy(ds)
        if 'StudyInstanceUID' not in ds:
            ds2.StudyInstanceUID = ''
        if 'SeriesInstanceUID' not in ds:
            ds2.SeriesInstanceUID = ''
        if ds2.StudyInstanceUID != '':
            ds2.QueryRetrieveLevel = 'SERIES'
            count = 0
            async for res in self._cFind(ds2, limit = limit):
                await publish(channel, "found.series", res)
                count += 1
            if count>0: await publish(channel, "found.study.series", ds2)
            return
        ds2.QueryRetrieveLevel = 'STUDY'
        responses = self._cFind(ds2)
        async for identifier in responses:
            ds3 = deepcopy(ds2)
            ds3.QueryRetrieveLevel = 'SERIES'
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            count = 0
            async for res in self._cFind(ds3, limit=limit):
                await publish(channel, "found.series", res)
                count += 1
            if count>0: await publish(channel, "found.study.series", identifier)

    async def _cFindInstances(self, channel, ds, limit = 0):
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
            for res in await self._cFind(ds2, limit = limit):
                if self.use_wado: uri = self._wadoURIURL(res,)
                else: uri=self._dimseURI()
                await publish(channel, "found.instance", res, uri=uri)
                count += 1
            if count>0: await publish(channel, "found.series.instances", ds2)
            return
        if ds2.StudyInstanceUID != '':
            ds2.QueryRetrieveLevel = 'SERIES'
            responses = self._cFind(channel, ds2)
            count0 = 0
            async for identifier in responses:
                ds3 = deepcopy(ds2)
                ds3.StudyInstanceUID = identifier.StudyInstanceUID
                ds3.SeriesInstanceUID = identifier.SeriesInstanceUID
                ds3.QueryRetrieveLevel = 'IMAGE'
                count1 = 0
                async for res in self._cFind(ds3, limit=limit):
                    if self.use_wado: uri = self._wadoURIURL(res)
                    else: uri=self._dimseURI()
                    await publish(channel, "found.instance", res, uri=uri)
                    count1 += 1
                if count1>0: await publish(channel, "found.series.instances", identifier)
                count0 += count1
            if count0>0: await publish(channel, "found.study.instances", ds2)
            return
        ds2.QueryRetrieveLevel = 'STUDY'
        responses = self._cFind(ds2)
        async for identifier in responses:
            ds3 = deepcopy(ds2)
            ds3.QueryRetrieveLevel = 'SERIES'
            ds3.StudyInstanceUID = identifier.StudyInstanceUID
            count0 = 0
            async for res in self._cFind(ds3, limit=limit):
                ds4 = deepcopy(ds3)
                ds4.StudyInstanceUID = res.StudyInstanceUID
                ds4.SeriesInstanceUID = res.SeriesInstanceUID
                ds4.QueryRetrieveLevel = 'IMAGE'
                count1 = 0
                async for res4 in self._cFind(ds4, limit=limit):
                    if self.use_wado: uri = self._wadoURIURL(res4)
                    else: uri=self._dimseURI()
                    await publish(channel, "found.instance", res4, uri=uri)
                    count1 += 1
                if count1>0: await publish(channel, "found.series.instances", res)
                count0 += count1
            if count0>0: await publish(channel, "found.study.instances", identifier)
        
async def dcmhandler(channel, ds, uri, method):
    scu = SCUfind()
    if method == 'find.studies':
        await scu._cFindStudy(channel, ds, limit=0)
    elif method == 'find.series':
        await scu._cFindSeries(channel, ds, limit=0)
    elif method == 'find.instances':
        await scu._cFindInstances(channel, ds, limit=0)


if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=[
            'find.#'
        ],
        dcmhandler=dcmhandler
    )