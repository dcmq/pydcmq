import asyncio
from pydicom import dcmread, dcmwrite, Dataset
from pydicom.tag import Tag
from pydicom.uid import generate_uid
import os 
import pathlib
import nibabel as nb
import numpy as np
from copy import deepcopy
from pydcmq import *

def nii2dicom(ni, ds):
    if len(ni.shape) == 3:
        I,J,K = ni.shape
        T=1
    elif len(ni.shape) == 4:
        I,J,K,T = ni.shape
    else:
        raise(Exception(f"tried to convert nifti with shape {ni.shape}"))

    orientation = ni.affine
    orientation33 = orientation[:3,:3]
    image_position0 = orientation[:3,3]

    pixel_spacing = list(map(np.linalg.norm, [orientation[:3,0], orientation[:3,1]]))
    slice_spacing = np.linalg.norm(orientation[:3,2])
    slice_thickness = ni.header["pixdim"][3]
    orientation_rescaled = orientation33 @ np.diag([
        1/pixel_spacing[0],
        1/pixel_spacing[1],
        1/slice_spacing
    ])
    orientation_dcm = np.reshape(orientation_rescaled[:2,:], 6)
    slice_vector = np.cross(orientation_rescaled[0,:], orientation_rescaled[1,:]) 
    scl_slope = 1 if np.isnan(ni.header["scl_slope"]) else ni.header["scl_slope"]
    scl_inter = 0 if np.isnan(ni.header["scl_inter"]) else ni.header["scl_inter"]
    raw = ni.get_fdata()
    minvol = np.min(raw)
    maxvol = np.max(raw)
    raw -= minvol
    scl_inter += minvol * scl_slope
    raw *= 65536 / (maxvol - minvol)
    scl_slope *= (maxvol - minvol) / 65536
    minval = scl_slope * np.min(raw) + scl_inter
    maxval = scl_slope * np.max(raw) + scl_inter
    uintVOL = np.uint16(np.round(raw))

    out_dcms = []
    for k in range(K):
        for t in range(T):
            d = Dataset()
            for elem in ds:
                if elem.tag.group in [0x8, 0x10, 0x18, 0x20, 0x28, 0x32, 0x40]:
                    d[elem.tag] = deepcopy(elem)
            fix_meta_info(d)
            d.SOPInstanceUID = generate_uid()
            d.SeriesInstanceUID = generate_uid(entropy_srcs=[d.SeriesInstanceUID, d.SeriesDescription])
            d.PixelSpacing = pixel_spacing
            d.SeriesNumber += 1000
            d.SpacingBetweenSlices = slice_spacing
            d.SliceThickness = slice_thickness
            d.ImageOrientationPatient = list(orientation_dcm)
            d.ImagePositionPatient = list(image_position0 + (k-1)*slice_vector*slice_spacing)
            d.SliceLocation = np.dot(d.ImagePositionPatient, slice_vector)
            d.Rows = J
            d.Columns = I
            d.InstanceNumber = k
            d.AcquisitionNumber = t
            if len(ni.shape) == 3:
                d.PixelData = np.flip(uintVOL[:,:,k]).T.copy(order='C').tobytes()
            else:
                d.PixelData = np.flip(uintVOL[:,:,k,t]).T.copy(order='C').tobytes()
            #d.Pixel Data = transpose(uintVOL[end:-1:1,end:-1:1,k,t])        # flip from RAS to LPS
            d.BitsAllocated = 16
            d.BitsStored = 16
            d.HighBit = 15
            d.PixelRepresentation = 0
            d.WindowCenter = (maxval + minval) / 2
            d.WindowWidth = (maxval - minval)
            d.WindowCenterWidthExplanation = "LINEAR"
            d.RescaleIntercept = scl_inter
            d.RescaleSlope = scl_slope
            fix_meta_info(d)
            out_dcms.append(d)
    return out_dcms

async def dcmhandler(channel, ds, uri, routing_key):
    if not Tag("ImageType") in ds or not "RESAMPLED" in ds.ImageType: #only convert resampled data
        print(f"dicom2nii: {uri} ({ds.SeriesDescription}) is not a resampled image")
        return
    print(f"nii2dicom: converting {uri}")
    ni = nb.load(uri)
    dicoms = nii2dicom(ni, ds)
    refds = dicoms[0]
    outdir = f"{os.environ['HOME']}/.dcmq/derived/{refds.StudyInstanceUID}/{refds.SeriesInstanceUID}"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    for dcm in dicoms:
        filepath = outdir + "/" + dcm.SOPInstanceUID
        dcmwrite(filepath, dcm, write_like_original=False)
        await publish(channel, "stored.instance", dcm, uri=filepath)
    await publish(channel, "stored.series", refds, uri=outdir)
        
if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=['stored.series.nii'],
        dcmhandler=dcmhandler
    )