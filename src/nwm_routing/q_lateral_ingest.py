# From https://registry.opendata.aws/nwm-archive/
import xarray as xr
import fsspec
import numpy as np
from dask.distributed import Client

# Set up connection
client = Client()
client
url = 's3://noaa-nwm-retro-v2-zarr-pds'
ds = xr.open_zarr(fsspec.get_mapper(url, anon=True), consolidated=True)

# grab var of intrest
var='q_lateral'
qlat_maxwet = ds[var].sel(time=slice('2010-04-01 00:00','2010-04-30 23:00'))
qlat_maxwet = qlat_maxwet.max(dim='time').compute()

# Export as ??
