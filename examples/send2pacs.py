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

transfer_syntax = [ExplicitVRLittleEndian,
                   ImplicitVRLittleEndian,
                   DeflatedExplicitVRLittleEndian,
                   ExplicitVRBigEndian]

queued = []
lastsend = time.time()

def _cStore(ds_queue):
    if len(ds_queue) == 0:
        return []
    ae = AE()
    if ds_queue[0].file_meta.TransferSyntaxUID not in transfer_syntax:
        for context in StoragePresentationContexts:
            ae.add_requested_context(context.abstract_syntax, [ds_queue[0].file_meta.TransferSyntaxUID])
    else:
        for context in StoragePresentationContexts:
            ae.add_requested_context(context.abstract_syntax, transfer_syntax)
    # Associate with peer AE
    assoc = ae.associate("127.0.0.1", 4006)

    n_sent = 0
    if assoc.is_established:
        # Use the C-STORE service to send the dataset
        # returns the response status as a pydicom Dataset
        for ds in ds_queue:
            status = assoc.send_c_store(ds)

            # Check the status of the storage request
            if status:
                # If the storage request succeeded this will be 0x0000
                print('C-STORE request status: 0x{0:04x}'.format(status.Status))
                n_sent += 1
            else:
                print('Connection timed out, was aborted or received invalid response')
                break
        assoc.release()
    else:
        print('Association rejected, aborted or never connected')
    assoc.release()
    return ds_queue[n_sent:]

async def dcmhandler(channel, ds, uri, routing_key):
    global queued
    global lastsend
    if ds.Modality == "SR":
        return
    try:
        ds_full = pydicom.dcmread(uri)
    except Exception as e:
        print(f"dcmread failed with {e}")
        return
    queued.append(ds_full)
    if time.time() - lastsend > 1: #only send every second
        queued = _cStore(queued)
        lastsend = time.time()
    else:
        await asyncio.sleep(1)
        queued = _cStore(queued)

        
if __name__ == '__main__':
    assoc = None
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=['stored.instance'],
        dcmhandler=dcmhandler
    )