#!/usr/bin/env python3

import pandas as pd
import geopandas as gpd
import os
import requests, zipfile, io
from pathlib import Path


def get_midpoints(streams,outfile):
    # Get stream midpoint
    stream_midpoints = []
    stream_ids = []
    for i,segment in streams.iterrows():
        stream_midpoints = stream_midpoints + [segment.geometry.interpolate(0.5,normalized=True)]
        stream_ids = stream_ids + [segment.COMID]
    
    crossections = gpd.GeoDataFrame({'geometry' : stream_midpoints ,'COMID':stream_ids}, crs=proj, geometry='geometry')
    crossections.to_file(outfile,driver='GPKG')

bathy_dir = '/media/sf_inland_routing/bathy'
bathy_dir = Path(bathy_dir)

## dissolve eHydro polygons
ms_eHydro_data_filename = bathy_dir / 'ehydro_domain' / 'mississippi_river_ehydro_subset.gpkg'
oh_eHydro_data_filename = bathy_dir / 'ehydro_domain' / 'ohio_river_ehydro_subset.gpkg'

proj = 'EPSG:4269'
url = 'sourcedatalocation'
ms_eHydro_data = gpd.read_file(ms_eHydro_data_filename)
oh_eHydro_data = gpd.read_file(oh_eHydro_data_filename)

# Reproject eHydro
ms_eHydro_data = ms_eHydro_data.to_crs(proj)
oh_eHydro_data = oh_eHydro_data.to_crs(proj)

ms_eHydro_data_diss = ms_eHydro_data.dissolve()
oh_eHydro_data_diss = oh_eHydro_data.dissolve()

## Clip nwm streamlines with dissolved eHydro data and get unique IDs
# read in NWM streams
nwm_streams_filename = '/media/sf_inland_routing/nwm_v2_1_Features.gpkg'
nwm_streams = gpd.read_file(nwm_streams_filename, layer = 'NHDFlowline_Network')


# Mask with eHydro boundaries
ms_nwm_streams = gpd.read_file(nwm_streams_filename, layer = 'NHDFlowline_Network',mask=ms_eHydro_data_diss)
oh_nwm_streams = gpd.read_file(nwm_streams_filename, layer = 'NHDFlowline_Network',mask=oh_eHydro_data_diss)

ms_midpoint_filename = bathy_dir / 'ehydro_domain' / 'mississippi_nwm_midpoints.gpkg'
oh_midpoint_filename = bathy_dir / 'ehydro_domain' / 'ohio_nwm_midpoints.gpkg'

## Get midpoints for crossections
get_midpoints(ms_nwm_streams,ms_midpoint_filename)
get_midpoints(oh_nwm_streams,oh_midpoint_filename)

# LM_08_MVF_20170712_CS_MB_48125_48175.ZIP
# 'https://ehydrotest.blob.core.usgovcloudapi.net/ehydro-surveys/CEMVN/MR_08_MED_20170809_CS.ZIP'
ms_aggregated_surveypoints_filename = bathy_dir / 'ehydro_domain' / 'ms_aggregated_surveypoints.gpkg'
download_dir = bathy_dir / 'ehydro_download'
## download eHydro data
for i,segment in ms_eHydro_data.iterrows():
    url = segment['sourcedatalocation']
    gdb_name = download_dir / str(url.split('/')[-1].replace('ZIP','gdb'))
    if not os.path.exists(gdb_name):
        print (f"downloading {url.split('/')[-1]}; {100 * (i+1/len(ms_eHydro_data))}% Complete")
        try: 
            # download data
            r = requests.get(url)
            # # extract data
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(download_dir)
        except Exception as e: print(repr(e))
    # open survey points in GDB
    try:
        surveypoints = gpd.read_file(gdb_name, layer='SurveyPoint')
    except:
        print (f"survey points layer does not exist for ehydro site {url.split('/')[-1]}")
    try:
        surveypoints = surveypoints.to_crs(proj)
        surveypoints = surveypoints[['SurveyDateStamp', 'SurveyId', 'Z_label', 'surveyPointElev','elevationDatum', 'elevationUOM', 'sourceType','geometry']]
        
        # Write out surveypoints to GPKG
        if os.path.isfile(ms_aggregated_surveypoints_filename):
            surveypoints.to_file(ms_aggregated_surveypoints_filename,driver='GPKG',index=False, mode='a')
        else:
            surveypoints.to_file(ms_aggregated_surveypoints_filename,driver='GPKG',index=False)
        
    except Exception as e:
        print (f"problem appending ehydro site {url.split('/')[-1]}; shape: {surveypoints.shape}; ")
        print (repr(e))
    
