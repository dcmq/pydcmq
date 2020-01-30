import asyncio
from pydicom import dcmread, dcmwrite
from pydicom.tag import Tag
import os, sys
import tempfile
from pathlib import Path
import numpy as np
from scipy.io import savemat, loadmat
from scipy.linalg import qr
from copy import deepcopy
from dcmq import consumer_loop, publish_nifti, publish_nifti_study, publish_dcm_series

localdir = Path(os.path.dirname(os.path.realpath(__file__)))

def isthinheadct(d):
    if not "SeriesDescription" in d:
        return False
    if d.SeriesDescription.startswith("Head  0.75") or \
        d.SeriesDescription.startswith("HeadSeq  1.2") or \
        d.SeriesDescription.startswith("Headseq  1.2") or \
        d.SeriesDescription.startswith("Head  1.0"):
        return True
    return False

def antsAffineToOrthogonal(infilename, outfilename):
    m = loadmat(infilename)
    affine = np.reshape(m["AffineTransform_double_3_3"][:9,0], (3,3))
    Q,R = qr(affine)
    for i in range(3):
        if R[i,i] < 0:
            Q[:,i] *= -1
    m["AffineTransform_double_3_3"][:9,0] = np.reshape(Q,9)

    savemat(outfilename, m, format='4')
    #need to write it explicitly because ANTs depends on the order of the variables
    # fout = open(outfilename, "w")
    # savemat(fout, {"AffineTransform_double_3_3": m["AffineTransform_double_3_3"]}, format='4')
    # savemat(fout, {"fixed": m["fixed"]}, format='4')
    # fout.close()

async def dcmhandler(channel, ds, uri):
    if not Tag("ImageType") in ds or not "PRIMARY" in ds.ImageType: #only convert primary data
        print(f"ctautorecon: {uri} is not a primary image")
        return
    if not isthinheadct(ds):
        print(f"ctautorecon: {uri} is not for autorecon")
        return
    print(f"ctautorecon: converting {uri}")
    niidir = Path.home() / ".dimseweb" / "nii" / ds.StudyInstanceUID
    with tempfile.TemporaryDirectory() as tempdir:
        path = Path(tempdir)
        os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = "4"
        mni = localdir / "scct_unsmooth.nii.gz"
        mni_hd = localdir / "scct_unsmooth_0.5_pad10.nii.gz"
        ct = uri
        cmd = f"antsRegistration --dimensionality 3 --output {path}/ct2mni --interpolation Linear --winsorize-image-intensities \[0.005,0.995\] --use-histogram-matching 1 --initial-moving-transform \[{mni},{ct},1\] --transform Rigid\[0.1\] --metric MI\[{mni},{ct},1,32,Regular,0.25\] --convergence \[1000x500x250,1e-6,10\] --shrink-factors 8x4x2 --smoothing-sigmas 3x2x1vox --transform Affine\[0.1\] --metric MI\[{mni},{ct},1,32,Regular,0.25\] --convergence \[1000x500x250x100,1e-6,10\] --shrink-factors 8x4x2x1 --smoothing-sigmas 3x2x1x0vox -v"
        print(cmd)
        os.system(cmd)
        ct2mni = path / "ct2mni0GenericAffine.mat"
        ct2mni_orthogonal = path / "ct2mni0GenericOrthogonal.mat"
        antsAffineToOrthogonal(ct2mni, ct2mni_orthogonal)
        out = niidir / (ds.SeriesInstanceUID + "MNI.nii.gz")
        cmd = f"antsApplyTransforms -i {ct} -r {mni_hd} -o {out} -t {ct2mni_orthogonal} --interpolation Linear -v -f -1024"
        print(cmd)
        os.system(cmd)
    print(f"ctautorecon: finished converting {uri}")
    ds.SeriesInstanceUID += ".12.13.8" #numeric code for MNI
    ds.SeriesDescription += " " + "MNI"
    oldimagetype = ds.ImageType
    oldimagetype[0] = "DERIVED"
    oldimagetype[1] = "SECONDARY"
    ds.ImageType = oldimagetype
    await publish_nifti(channel, ds, str(out))
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="ctautorecon",
        methods=['stored.series.nii'],
        dcmhandler=dcmhandler
    )