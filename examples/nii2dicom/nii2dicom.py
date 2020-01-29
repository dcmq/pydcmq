import asyncio
from pydicom import dcmread, dcmwrite
import os 
import pathlib
import nibabel as nb
import numpy as np
from copy import deepcopy
from dcmq import consumer_loop, publish_nifti, publish_nifti_study, publish_dcm_series

def nii2dicom(ni, ds, rescale=False, windowing=False, name=""):
    if not "DERIVED" in ds.ImageType: #only convert derived data
        return
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
    meanvol = np.mean(raw)
    stdvol = np.std(raw)
    minvol = np.min(raw)
    maxvol = np.max(raw)
    if minvol < 0:
        raw -= minvol
        scl_inter += minvol * scl_slope
    if maxvol > 65535: # 65535 = maximum of uint16
        raw -= (maxvol - 65535)
        scl_inter += (maxvol - 65535) * scl_slope
    uintVOL = np.uint16(np.round(raw))

    out_dcms = []
    for k in range(K):
        for t in range(T):
            d = deepcopy(ds)
            oldimagetype = d.ImageType
            oldimagetype[0] = "DERIVED"
            oldimagetype[1] = "SECONDARY"
            d.ImageType = oldimagetype
            d.SOPInstanceUID += "." + str(k) + "." + str(t)
            d.file_meta.MediaStorageSOPInstanceUID = d.SOPInstanceUID
            d.SeriesInstanceUID += name
            d.SeriesDescription += " " + name
            d.PixelSpacing = pixel_spacing
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
                d.PixelData = np.flip(uintVOL[:,:,k]).T.copy(order='C')
            else:
                d.PixelData = np.flip(uintVOL[:,:,k,t]).T.copy(order='C')
            #d.Pixel Data = transpose(uintVOL[end:-1:1,end:-1:1,k,t])        # flip from RAS to LPS
            d.BitsAllocated = 16
            d.BitsStored = 16
            d.HighBit = 15
            tag = ds.data_element("OtherPatientIDs").tag
            del d.SmallestImagePixelValue
            del d.LargestImagePixelValue
            d.PixelRepresentation = 0
            if windowing:
                d.WindowCenter = meanvol
                d.WindowWidth = 4*stdvol
                d.WindowCenterWidthExplanation = "LINEAR"
            if rescale:
                d.RescaleIntercept = minvol
                d.RescaleSlope = (maxvol - minvol)/65536
            else:
                d.RescaleIntercept = scl_inter
                d.RescaleSlope = scl_slope
            out_dcms.append(d)
    return out_dcms

async def dcmhandler(channel, ds, uri):
    print(f"nii2dicom: converting {uri}")
    ni = nb.load(uri)
    dicoms = nii2dicom(ni, ds, name=".13.8.8") #numbers for 'nii'
    outdir = f"{os.environ['HOME']}/.dimseweb/derived/{ds.StudyInstanceUID}/{ds.SeriesInstanceUID}"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    for dcm in dicoms:
        dcmwrite(outdir + "/" + dcm.SOPInstanceUID, dcm)
    await publish_dcm_series(channel, ds, outdir)
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="nii2dicom",
        methods=['stored.series.nii'],
        dcmhandler=dcmhandler
    )