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
from pydcmq import *

localdir = Path(os.path.dirname(os.path.realpath(__file__)))

def isthinheadct(d):
    if not "SliceThickness" in d:
        return False
    if d.SliceThickness == None:
        return False
    for s in ["Topo", "Angio", "Neck", "Thorax"]:
        if s in d.SeriesDescription:
            return False
    if float(d.SliceThickness) < 2.0 and d.Modality == "CT":
        return True
    return False

async def run(cmd):
    loop = asyncio.get_running_loop()
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        loop=loop)

    stdout, stderr = await proc.communicate()

    print(f'[{cmd!r} exited with {proc.returncode}]')
    if stdout:
        print(f'[stdout]\n{stdout.decode()}')
    if stderr:
        print(f'[stderr]\n{stderr.decode()}')

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
        print(f"ctautorecon: {uri} ({ds.SeriesDescription}) is not a primary image")
        return
    if not isthinheadct(ds):
        print(f"ctautorecon: {uri} ({ds.SeriesDescription}) is not for autorecon")
        return
    print(f"ctautorecon: converting {uri} ({ds.SeriesDescription})")
    niidir = Path.home() / ".dcmq" / "nii" / ds.StudyInstanceUID
    with tempfile.TemporaryDirectory() as tempdir:
        path = Path(tempdir)
        os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = "1"
        mni = localdir / "scct_unsmooth.nii.gz"
        mni_hd = localdir / "scct_unsmooth_0.5_pad10.nii.gz"
        ct = uri
        cmd = f"antsRegistration --dimensionality 3 --output {path}/ct2mni --interpolation Linear --winsorize-image-intensities \[0.005,0.995\] --use-histogram-matching 1 --initial-moving-transform \[{mni},{ct},1\] --transform Rigid\[0.1\] --metric MI\[{mni},{ct},1,32,Regular,0.25\] --convergence \[1000x500x250,1e-6,10\] --shrink-factors 8x4x2 --smoothing-sigmas 3x2x1vox --transform Affine\[0.1\] --metric MI\[{mni},{ct},1,32,Regular,0.25\] --convergence \[1000x500x250x100,1e-6,10\] --shrink-factors 8x4x2x1 --smoothing-sigmas 3x2x1x0vox -v"
        print(cmd)
        await run(cmd) #os.system(cmd)
        ct2mni = path / "ct2mni0GenericAffine.mat"
        ct2mni_orthogonal = path / "ct2mni0GenericOrthogonal.mat"
        antsAffineToOrthogonal(ct2mni, ct2mni_orthogonal)
        out = niidir / (ds.SeriesInstanceUID + "MNI.nii.gz")
        cmd = f"antsApplyTransforms -i {ct} -r {mni_hd} -o {out} -t {ct2mni_orthogonal} --interpolation Linear -v -f -1024"
        print(cmd)
        await run(cmd) #os.system(cmd)
    print(f"ctautorecon: finished converting {uri} ({ds.SeriesDescription})")
    ds.SeriesDescription += " MNI"
    oldimagetype = ds.ImageType
    oldimagetype[0] = "DERIVED"
    oldimagetype[1] = "SECONDARY"
    oldimagetype[2] = "RESAMPLED"
    ds.ImageType = oldimagetype
    await publish(channel, "stored.series.nii", ds, uri=str(out))
        
if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="ctautorecon",
        methods=['stored.series.nii'],
        dcmhandler=dcmhandler
    )