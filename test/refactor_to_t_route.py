#!/usr/bin/env python3

import argparse
import os
import geopandas as gpd
import pandas as pd
import json
from pathlib import Path
import time
from evaluate_reference_hydrofabric import eval_reference_hydrofabric
from shared_functions import generate_qlat, recalculate_flows, convert_results_to_df, run_troute_from_script, add_routelink_attributes, split_routelink_waterbodies, clean_dataset, replace_line, create_cn_summary_table

'''
Notes for Mike:
    * mising segments in route link file
    * reference hydrofabirc formatting: can we run the same processing script on the reference fabric to generate similar attributes
TODOs:
    * create formal template for evaluation metrics
'''


def run_troute(huc_id,nts,testing_dir,qlat_type):

    '''
    1. Populate a refactored stream network with channel routing properties from NWM v2.1 route link file.
       Subset by available channel routing properties.
    2. Run MC/diffusive wave routing test on the Hurricane Florence Domain to calculate Courant Numbers for each segment for every timestep (separate script)
    3. Clean up CN output format and generate summary statistics

    reporting
    1) CN violations: segment count: timesteps
    2) min, max, average, median stream length for the original and refactored networks
    3) check number of unique segments that violate CN (account for multiple time step violations)
    4) % Length of stream network with CN violations over period of time
    5) network order -- if we could bin by order, that might be interesting
    '''

    testing_dir = Path(testing_dir)
    # default_yaml_filename = testing_dir / 'refactored_test.yaml'

    inputs_dir = testing_dir / "inputs"
    routlink_netcdf_filename = inputs_dir / "v2.1" / 'RouteLink_CONUS.nc'
    refactored_streams_dir = inputs_dir / "refactored" / str(huc_id)
    refactored_hydrofabrics = os.listdir(refactored_streams_dir)
               
    outputs_dir = testing_dir / "outputs" / str(huc_id)
    if not Path(outputs_dir).is_dir():
        os.mkdir(Path(outputs_dir))
    # aggregate_cn_table_filename = outputs_dir / 'aggregate_cn_table.csv'
    aggregate_cn_summary_table_filename = outputs_dir / 'aggregate_cn_summary_table.csv'
    
    diagnostic_dir = outputs_dir / 'diagnostic'
    if not Path(diagnostic_dir).is_dir():
        os.mkdir(Path(diagnostic_dir))
    else: 
        files = list(diagnostic_dir.glob('**/*.json'))
        for f in files:
            os.remove(f)

    # Calculate base metrics
    default_yaml_filename = inputs_dir / 'yaml' / str('reference_' + huc_id + '.yaml')
    if not os.path.isfile(default_yaml_filename):
        
        eval_reference_hydrofabric(testing_dir,qlat_type, huc_id)

    # Calculate metrics for each refactored hydrofabrics
    for hydrofabric in refactored_hydrofabrics:
        print (f"running hydrofabric {hydrofabric}")

        # Read in refactored stream network
        refactored_streams = gpd.read_file(Path(refactored_streams_dir) / hydrofabric)

        if not set(["NHDWaterbodyComID","alt"]).issubset(refactored_streams.columns):
            # Populate stream network with channel routing properties from NWM v2.1 route link file.
            refactored_streams, ids_missing_parameters, comid_set, fraction_dict = add_routelink_attributes(refactored_streams, routlink_netcdf_filename)

            with open(Path(testing_dir / str(hydrofabric + '_comid_set.lst')), 'w') as f:
                for comid in list(comid_set):
                    f.write("%s\n" % comid)

            with open(Path(diagnostic_dir / str(hydrofabric + '_comid_set.json')), 'w') as f:
                json.dump(fraction_dict, f)

            print(f"{len(ids_missing_parameters)} segments were not in routelink file")
                    
        # Remove NA segments and clean up column names (this can be improved)
        refactored_streams = refactored_streams.dropna(subset=['n','toID'])
        refactored_streams = refactored_streams.rename(columns={'Length_m': 'Length', 'ID': 'link', 'toID': 'to'})
        
        if refactored_streams.to.dtype != 'int': refactored_streams.to = refactored_streams.to.astype(int)
        
        refactored_streams.NHDWaterbodyComID = refactored_streams.NHDWaterbodyComID.fillna(value='-9999')
        refactored_streams = split_routelink_waterbodies(refactored_streams) # TODO: develop a method for consolidating lake features
        
        if refactored_streams.NHDWaterbodyComID.dtype != 'int': refactored_streams.NHDWaterbodyComID = refactored_streams.NHDWaterbodyComID.astype(int)
            
        # Save updated gpkg
        clean_up_dir = inputs_dir / "refactored_cleaned_up"
        if not clean_up_dir.is_dir():
            os.mkdir(clean_up_dir)
        refactored_streams_filename = Path(clean_up_dir) / hydrofabric
        refactored_streams.to_file(refactored_streams_filename, layer='refactored',driver='GPKG')
        
        # Create refactored q_lats
        orig_q_lat_filename = inputs_dir / "v2.1" / "q_lat" / str('reference_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv')
        orig_q_lat = pd.read_csv(orig_q_lat_filename)
        refactored_q_lat, qlat_ids_missing_parameters, qlat_fraction_dict = generate_qlat(refactored_streams, orig_q_lat) # , qlat_comid_set
        
        with open(Path(diagnostic_dir / str(hydrofabric + '_qlat_fraction_dict.json')), 'w') as f:
            json.dump(qlat_fraction_dict, f)
            
        # Export as csv
        hydrofabric_version, layer_type = hydrofabric.split('.')
        refactored_qlat_filename = str(hydrofabric_version) + '_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv'
        refactored_q_lat.to_csv(inputs_dir / "v2.1" / 'q_lat' / refactored_qlat_filename)
            
        ## Create t-route input file
        yaml_dict = {}
        yaml_dict["title_string"] = "    title_string:" +  " refactored_" + hydrofabric_version + "\n"
        yaml_dict["geo_file_path"] = '    geo_file_path: ' +  '\"' + str(refactored_streams_filename) + '\"' + '\n'
        yaml_dict["layer_string"] = '    layer_string: refactored' + '\n'
        
        if qlat_type == 'max':
            yaml_dict["qlat_const"] = '    qlat_const: ' +  '\"' + str(inputs_dir / "v2.1" / 'q_lat' / str(str(hydrofabric_version) + '_qlat_' + str(qlat_type) + '_' + str(huc_id) + '.csv')) + '\"' + '\n'            
        else:
            yaml_dict["qlat_input_file"] = '    qlat_input_file: ' +  '\"' + str(inputs_dir / "v2.1" / 'q_lat' / refactored_qlat_filename) + '\"' + '\n'    

        output_filename = outputs_dir / str('refactored_' + hydrofabric_version + '.yaml')

        # Write yaml file
        replace_line(default_yaml_filename, yaml_dict, output_filename)

        ## Run MC/diffusive wave routing test to calculate Courant Numbers for each segment for every timestep (calls compiled Fortran module)
        # Run t-route
        start_t_route_wall_clock = time.time()
        t_route_results = run_troute_from_script(str(output_filename))
        end_t_route_wall_clock  = time.time()
        print(f"{hydrofabric_version} t-route run time: {end_t_route_wall_clock  - start_t_route_wall_clock}")
        # Convert results to dataframe
        fvd, courant = convert_results_to_df(t_route_results, nts, True)

        ## Clean up Courant Number output format and generate summary statistics
        tidy_network_cn = clean_dataset(courant)
        tidy_network_cn['parameters'] = hydrofabric_version

        # rpu, split_flines_meters, collapse_flines_meters, collapse_flines_main_meters = hydrofabric_version.split('_')
        # tidy_network_cn['rpu'] = rpu
        # tidy_network_cn['split_flines_meters'] = split_flines_meters
        # tidy_network_cn['collapse_flines_meters'] = collapse_flines_meters
        # tidy_network_cn['collapse_flines_main_meters'] = collapse_flines_main_meters

        # # Write out CN values to aggregate table
        # if os.path.isfile(aggregate_cn_table_filename):
        #     tidy_network_cn.to_csv(aggregate_cn_table_filename,index=False, mode='a')
        # else:
        #     tidy_network_cn.to_csv(aggregate_cn_table_filename,index=False)

        cn_summary_table = create_cn_summary_table(tidy_network_cn, refactored_streams, hydrofabric_version)

        if os.path.isfile(aggregate_cn_summary_table_filename):
            cn_summary_table.to_csv(aggregate_cn_summary_table_filename,index=False, mode='a',header=False)
        else:
            cn_summary_table.to_csv(aggregate_cn_summary_table_filename,index=False)

        del refactored_streams, t_route_results, fvd, courant, tidy_network_cn

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Run t-route routing algorithms and calculate metrics')
    parser.add_argument('-huc', '--huc-id', help='HUC2 ID',required=True,type=str)
    parser.add_argument('-nts', '--nts', help='number of timesteps',required=True,type=int)
    parser.add_argument('-d', '--testing-dir', help='testing directory',required=True)
    parser.add_argument('-qlat_type', '--qlat-type', help='q_lat data type (timeseries or max)',required=False,default='ts')

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    run_troute(**args)
