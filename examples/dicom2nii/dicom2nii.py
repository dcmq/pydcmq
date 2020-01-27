import asyncio
import dicom2nifti
from pydicom import dcmread
import os 
import pathlib
from dcmq import consumer_loop, publish_nifti, publish_nifti_study
import dicom2nifti.settings as settings

settings.disable_validate_orthogonal()
settings.disable_validate_slice_increment()
settings.enable_resampling()
settings.set_resample_spline_interpolation_order(1)
settings.set_resample_padding(-1000)

async def dcmhandler(channel, ds, uri):
    print(f"dicom2nii: converting {uri}")
    outdir = f"{os.environ['HOME']}/.dimseweb/nii/{ds.StudyInstanceUID}"
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)
    with os.scandir(uri) as it: 
        for series in it: 
            if series.is_dir():
                outfile = os.path.join(outdir, series.name + ".nii")
                try:
                    dicom2nifti.dicom_series_to_nifti(series, outfile, reorient_nifti=True)
                except Exception as e:
                    print(f"dicom2nii: error converting {series.name}: {e}")
                    continue
                with os.scandir(series) as it2: 
                    for instance in it2:
                        if instance.is_file():
                            dcmfilename = instance.name
                            break
                refds = dcmread(os.path.join(uri,series.name,dcmfilename))
                await publish_nifti(channel, refds, outfile)
    await publish_nifti_study(channel, ds, outdir)
        
if __name__ == '__main__':
    consumer_loop(
        server="amqp://guest:guest@127.0.0.1/",
        queue="dicom2nii",
        methods=['stored.study'],
        dcmhandler=dcmhandler
    )