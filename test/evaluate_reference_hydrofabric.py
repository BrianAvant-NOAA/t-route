#!/usr/bin/env python3

import argparse
import os
import geopandas as gpd
import xarray as xr
from pathlib import Path
import time
import fsspec
from shared_functions import replace_line, run_troute_from_script, convert_results_to_df, create_cn_summary_table, clean_dataset

'''
Retrieve q_lat values for each segment (https://registry.opendata.aws/nwm-archive/)
Run t-route MC over time series and calculate Courant number violations
Summarize network and generate summary table
'''
 

def eval_reference_hydrofabric(testing_dir,qlat_type,huc_id):
        
    # Set up connectionclient = Client()
    url = 's3://noaa-nwm-retro-v2-zarr-pds'
    ds = xr.open_zarr(fsspec.get_mapper(url, anon=True), consolidated=True)
    
    # Get q_lat data (m3 s-1)
    qlat_ts = ds['q_lateral'].sel(time=slice('2018-09-01 00:00:00','2018-09-30 23:00:00')) # '2018-09-01 02:00:00','2018-09-30 23:00:00' # '2010-04-01 00:00','2010-04-30 23:00'
    qlat_ts = qlat_ts.drop(labels=['longitude','latitude'])

    testing_dir = Path(testing_dir)
    default_yaml_filename = testing_dir / 'refactored_test.yaml'
    
    inputs_dir = testing_dir / "inputs"
    routlink_netcdf_filename = inputs_dir / "v2.1" / 'RouteLink_CONUS.nc'
    nwm_v2_1_streams_streams_dir = inputs_dir / "v2.1" / 'ngen-reference'
    
    if huc_id==None:
        reference_hydrofabrics = os.listdir(nwm_v2_1_streams_streams_dir)
    else:
        reference_hydrofabrics = list(nwm_v2_1_streams_streams_dir / str('ngen_reference_' + str(huc_id) + '.gpkg'))
    
    yaml_dir = inputs_dir / 'yaml'
    if not Path(yaml_dir).is_dir():
        os.mkdir(Path(yaml_dir))
    else: 
        files = list(yaml_dir.glob('**/*.yaml'))
        for f in files:
            os.remove(f)
            
    # Calculate base metrics
    for reference_hydrofabric in reference_hydrofabrics:
        
        nwm_v2_1_streams_filename = nwm_v2_1_streams_streams_dir / reference_hydrofabric
        version, ref, huc_id = reference_hydrofabric.split('_')
        huc_id, ext = huc_id.split('.')
        
        outputs_dir = testing_dir / "outputs" / str(huc_id)
        if not Path(outputs_dir).is_dir():
            os.mkdir(Path(outputs_dir))
            
        aggregate_cn_summary_table_filename = outputs_dir / 'aggregate_cn_summary_table.csv'
        nwm_v2_1_streams_subset_filename = outputs_dir / str("routlink_" + str(huc_id) + ".gpkg")
        
        diagnostic_dir = outputs_dir / 'diagnostic'
        if not Path(diagnostic_dir).is_dir():
            os.mkdir(Path(diagnostic_dir))
        else: 
            files = list(diagnostic_dir.glob('**/*.json'))
            for f in files:
                os.remove(f)
        
        nwm_v2_1_streams = gpd.read_file(nwm_v2_1_streams_filename,layername='flowpaths') # ,dtype={'NHDWaterbodyComID':str}
        if nwm_v2_1_streams.featureid.dtype != 'int': nwm_v2_1_streams.featureid = nwm_v2_1_streams.featureid.astype(int)
        subset_ids = nwm_v2_1_streams.featureid.to_list()
        
        routlink_netcdf = xr.open_dataset(routlink_netcdf_filename)
        routlink_df = routlink_netcdf.to_dataframe()
        
        subset_routelink = routlink_df.loc[routlink_df.link.isin(subset_ids)].copy()
        subset_routelink.reset_index(drop=True, inplace=True)
        
        # Subset relevant segments
        comids_list = sorted(subset_routelink['link'].to_list())    
        qlat_ts_subset = qlat_ts.sel(feature_id=(comids_list))
        
        # nts = min(288,len(qlat_ts_subset))
        nts = 288
            
        yaml_dict = {}
        yaml_dict["nts"] = "    nts: " + str(nts) + '\n'
        yaml_dict["title_string"] = "    title_string: " +  "routlink_" + huc_id + "\n"
        yaml_dict["geo_file_path"] = '    geo_file_path: ' +  '\"' + str(nwm_v2_1_streams_subset_filename) + '\"' + '\n'
        yaml_dict["layer_string"] = '    layer_string: flowpaths' + '\n'
        
        if qlat_type == 'max':
        
            # Calculate max q_lat over time series
            qlat_maxwet = qlat_ts_subset.max(dim='time').compute()
            
            # Convert to dataframe
            qlat_maxwet_df = qlat_maxwet.to_dataframe()
            qlat_maxwet_df.index.name = None
            
            # Export as csv
            qlat_maxwet_df.to_csv(inputs_dir / "v2.1" / 'q_lat' /str('reference_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv'))
            
            yaml_dict["# qlat_const"] = '    qlat_const: ' +  '\"' + str(inputs_dir / "v2.1" / 'q_lat' / str('reference_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv')) + '\"' + '\n'            
            
            del qlat_maxwet_df, qlat_maxwet
            
        if qlat_type == 'ts':
            
            # Convert to dataframe
            qlat_ts_df = qlat_ts_subset.to_dataframe()
                
            # Reformat dataframe
            qlat_ts_df.reset_index(inplace=True)
            qlat_ts_df_pivot = qlat_ts_df.pivot(index='feature_id', columns='time', values='q_lateral')
            qlat_ts_df_pivot.index.name = None
            
            # Export as csv
            qlat_ts_df_pivot.to_csv(inputs_dir / "v2.1" / 'q_lat' / str('reference_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv'))
            
            yaml_dict["# qlat_input_file"] = '    qlat_input_file: ' +  '\"' + str(inputs_dir / "v2.1" / 'q_lat' / str('reference_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv')) + '\"' + '\n'    
        
            del qlat_ts_df_pivot, qlat_ts_df
                
        del qlat_ts_subset
        
        subset_routelink = nwm_v2_1_streams[['featureid','geometry']].merge(subset_routelink, left_on=['featureid'], right_on=['link'])
             
        subset_routelink = subset_routelink.drop(columns=['gages'])
        
        subset_routelink.to_file(nwm_v2_1_streams_subset_filename,layer='nwm_flowline',driver='GPKG')
        
        ## Create t-route input file
        output_filename = yaml_dir / str('reference_' + huc_id + '.yaml')
        
        # Write yaml file
        replace_line(default_yaml_filename, yaml_dict, output_filename)
    
        ## Run MC/diffusive wave routing test to calculate Courant Numbers for each segment for every timestep (calls compiled Fortran module)
        # Run t-route
        start_t_route_wall_clock = time.time()
        t_route_results = run_troute_from_script(str(output_filename))
        end_t_route_wall_clock  = time.time()
        print(f"reference_{huc_id} t-route run time: {end_t_route_wall_clock  - start_t_route_wall_clock}")
        # Convert results to dataframe
        fvd, courant = convert_results_to_df(t_route_results, nts, True)
    
        # Clean up Courant Number output table
        tidy_network_cn = clean_dataset(courant)
        tidy_network_cn['parameters'] = 'nwm_flowline'
    
        network = 'reference_' + huc_id
        cn_summary_table = create_cn_summary_table(tidy_network_cn, subset_routelink, network)
    
        cn_summary_table.to_csv(aggregate_cn_summary_table_filename,index=False)
    
        del nwm_v2_1_streams, subset_routelink, t_route_results, fvd, courant, tidy_network_cn
    
        # # Write out CN values to aggregate table
        # if os.path.isfile(aggregate_cn_table_filename):
        #     tidy_network_cn.to_csv(aggregate_cn_table_filename,index=False, mode='a',header=False)
        # else:
        #     tidy_network_cn.to_csv(aggregate_cn_table_filename,index=False)
    
    
if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Run t-route routing algorithms and calculate metrics')
    # parser.add_argument('-nts', '--nts', help='number of timesteps',required=True,type=int)
    parser.add_argument('-d', '--testing-dir', help='testing directory',required=True)
    parser.add_argument('-qlat_type', '--qlat-type', help='q_lat data type (timeseries or max)',required=True)
    parser.add_argument('-huc', '--huc-id', help='HUC2 ID',required=False,default=None)
    
    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())
        
    eval_reference_hydrofabric(**args)
