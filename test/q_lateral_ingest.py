#!/usr/bin/env python3

# From https://registry.opendata.aws/nwm-archive/
import xarray as xr
import fsspec
import numpy as np
from dask.distributed import Client
from pathlib import Path
import os
import geopandas as gpd

var='q_lateral'
huc_id = '01a'
testing_dir = "/media/sf_inland_routing/testing"

testing_dir = Path(testing_dir)

inputs_dir = testing_dir / "inputs"
routlink_netcdf_filename = inputs_dir / "v2.1" / 'RouteLink_CONUS.nc'
nwm_v2_1_streams_filename = inputs_dir / "v2.1" / 'ngen-reference' / str("ngen_reference_" + str(huc_id) + ".gpkg")
refactored_streams_dir = inputs_dir / "refactored" / str(huc_id)

nwm_v2_1_streams = gpd.read_file(nwm_v2_1_streams_filename,layername='flowpaths')
if nwm_v2_1_streams.featureid.dtype != 'int': nwm_v2_1_streams.featureid = nwm_v2_1_streams.featureid.astype(int)
subset_ids = nwm_v2_1_streams.featureid.to_list()

routlink_netcdf = xr.open_dataset(routlink_netcdf_filename)
routlink_df = routlink_netcdf.to_dataframe()

subset_routelink = routlink_df.loc[routlink_df.link.isin(subset_ids)].copy()
subset_routelink.reset_index(drop=True, inplace=True)

comids_list = sorted(subset_routelink['link'].to_list())

# Set up connectionclient = Client()
url = 's3://noaa-nwm-retro-v2-zarr-pds'
ds = xr.open_zarr(fsspec.get_mapper(url, anon=True), consolidated=True)

# grab var of interest
qlat_ts = ds[var].sel(time=slice('2010-04-01 00:00','2010-04-30 23:00'))
qlat_ts = qlat_ts.drop(labels=['longitude','latitude'])

# Subset relevant segments
qlat_ts_subset = qlat_ts.sel(feature_id=(comids_list))

# Calculate max q_lat over time series
qlat_maxwet = qlat_ts_subset.max(dim='time').compute()

# Convert to dataframe
qlat_ts_df = qlat_ts_subset.to_dataframe()
qlat_ts_df.q_lateral = np.round(qlat_ts_df.q_lateral,3)
qlat_maxwet_df = qlat_maxwet.to_dataframe()

# Reformat dataframe
qlat_ts_df.reset_index(inplace=True)
qlat_ts_df_pivot = qlat_ts_df.pivot(index='feature_id', columns='time', values='q_lateral')
qlat_ts_df_pivot.index.name = None

qlat_maxwet_df.index.name = None

# Export as csv
qlat_ts_df_pivot.to_csv(inputs_dir / "v2.1" / 'q_lat' / str('reference_qlat_ts_' + str(huc_id) + '.csv'))
qlat_maxwet_df.to_csv(inputs_dir / "v2.1" / 'q_lat' /str('reference_qlat_maxwet_' + str(huc_id) + '.csv'))
