from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
from pynetdicom import (
    AE,
    StoragePresentationContexts,
)
from pydicom.uid import (
    ExplicitVRLittleEndian, ImplicitVRLittleEndian,
    ExplicitVRBigEndian, DeflatedExplicitVRLittleEndian, 
    JPEGLossless
)
import pydicom
from pydicom.uid import UID
from pydicom import Dataset
from pydicom.tag import Tag
from pydcmq import consumer_loop
import asyncio

transfer_syntax = [ExplicitVRLittleEndian,
                   ImplicitVRLittleEndian,
                   DeflatedExplicitVRLittleEndian,
                   ExplicitVRBigEndian]

assoc = None

def _cStore(ds):
    if assoc != None and assoc.is_established:
        try:
            status = assoc.send_c_store(ds)
            if status and status.Status == 0:
                return assoc
        except Exception as e:
            print(e)
            print("error using assoc, creating a new one")
            pass

    ae = AE()
    if ds.file_meta.TransferSyntaxUID not in transfer_syntax:
        for context in StoragePresentationContexts:
            ae.add_requested_context(context.abstract_syntax, [ds.file_meta.TransferSyntaxUID])
    else:
        for context in StoragePresentationContexts:
            ae.add_requested_context(context.abstract_syntax, transfer_syntax)
    # Associate with peer AE
    assoc = ae.associate("127.0.0.1", 11112)

    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        status = assoc.send_c_store(ds)

        # Check the status of the storage request
        if status:
            # If the storage request succeeded this will be 0x0000
            print('C-STORE request status: 0x{0:04x}'.format(status.Status))
        else:
            print('Connection timed out, was aborted or received invalid response')
    else:
        print('Association rejected, aborted or never connected')


async def dcmhandler(channel, ds, uri):
    if ds.Modality == "SR":
        return
    ds_full = pydicom.dcmread(uri)
    _cStore(ds_full)
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="send2pacs",
        methods=['stored.instance'],
        dcmhandler=dcmhandler
    )