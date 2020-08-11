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
from pydcmq import subscriber_loop
import asyncio
import time
import os



def _cStore(ds_gen):
    ae = AE()
    dsets = list(ds_gen)
    transfer_syntax = [ExplicitVRLittleEndian,
                    ImplicitVRLittleEndian,
                    DeflatedExplicitVRLittleEndian,
                    ExplicitVRBigEndian]
    for context in StoragePresentationContexts:
        ae.add_requested_context(context.abstract_syntax, transfer_syntax)
    for ds in dsets:
        if ds.file_meta.TransferSyntaxUID not in transfer_syntax:
            for context in StoragePresentationContexts:
                try:
                    ae.add_requested_context(context.abstract_syntax, [ds.file_meta.TransferSyntaxUID])
                except: pass
            transfer_syntax += [ds.file_meta.TransferSyntaxUID]
    # Associate with peer AE
    assoc = ae.associate("127.0.0.1", 4006)

    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        for ds in dsets:
            status = assoc.send_c_store(ds)

            # Check the status of the storage request
            if status:
                # If the storage request succeeded this will be 0x0000
                print('C-STORE request status: 0x{0:04x}'.format(status.Status))
            else:
                print('Connection timed out, was aborted or received invalid response')
        assoc.release()
    else:
        print('Association rejected, aborted or never connected')

def ds_from_path(path):
    for root, _, files in os.walk(path):
        for name in files:
            try:
                yield pydicom.dcmread(os.path.join(root, name))
            except Exception as e:
                print(e)
                continue

async def dcmhandler(channel, ds, uri, routing_key):
    if ds.Modality == "SR":
        return
    try:
        path = uri
        _cStore(ds_from_path(path))
        
    except Exception as e:
        print(f"cStore failed with {e}")
        return

if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=['stored.series'],
        dcmhandler=dcmhandler
    )