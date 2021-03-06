import asyncio
from pydicom import dcmread
from pydicom.tag import Tag
import dicom2nifti
import os 
import pathlib
from pydcmq import *
import dicom2nifti.settings as settings

settings.disable_validate_orthogonal()
settings.disable_validate_slice_increment()
settings.enable_resampling()
settings.set_resample_spline_interpolation_order(1)
settings.set_resample_padding(-1024)

async def dcmhandler(channel, ds, uri, routing_key):
    print(f"dcm2niix: converting {uri} ({ds.SeriesDescription})")
    outdir = f"{os.environ['HOME']}/.dcmq/nii/{ds.StudyInstanceUID}"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    count = 0
    with os.scandir(uri) as it:
        for series in it: 
            if series.is_dir():
                with os.scandir(series) as it2: 
                    for instance in it2:
                        if instance.is_file():
                            dcmfilename = instance.name
                            break
                refds = dcmread(os.path.join(uri, series.name, dcmfilename))
                try:
                    if not Tag("ImageType") in refds or not "PRIMARY" in refds.ImageType or "RESAMPLED" in ds.ImageType: #only convert primary data
                        print(f"dcm2niix: {os.path.join(uri, series.name)} ({refds.SeriesDescription}) is not a primary image")
                        continue
                except Exception as e:
                    print(e)
                outfile = os.path.join(outdir, refds.SeriesInstanceUID + ".nii")
                try:
                    indir = os.path.join(uri, series.name)
                    cmd = f"dcm2niix -f %j -o {outdir} -b n -m y {indir}"
                    print(cmd)
                    ret = os.system(cmd)
                    if ret == 0:
                        if os.path.exists(os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_1.nii")):
                            outfile = os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_1.nii")
                        if os.path.exists(os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_Eq_1.nii")):
                            outfile = os.path.join(outdir, refds.SeriesInstanceUID + "_Tilt_Eq_1.nii")
                        if os.path.exists(outfile):
                            await publish(channel, "stored.series.nii", refds, uri=outfile)
                            count += 1
                        else:
                            print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): outfile not found")
                    else:
                        print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): exited with error code {ret}")
                        print(f"dcm2niix: retrying with dicom2nifti")
                        dicom2nifti.dicom_series_to_nifti(indir, outfile, reorient_nifti=True)
                        count += 1
                        await publish(channel, "stored.series.nii", refds, uri=outfile)
                except Exception as e:
                    print(f"dcm2niix: error converting {series.name} ({refds.SeriesDescription}): {e}")
                    continue
    if count>0:
        await publish(channel, "stored.study.nii", ds, uri=outdir)
        
if __name__ == '__main__':
    subscriber_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="",
        methods=['stored.study'],
        dcmhandler=dcmhandler
    )