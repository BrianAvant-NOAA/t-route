#!/usr/bin/env python3

import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=RuntimeWarning)
import xarray as xr
from nwm_routing.__main__ import _run_everything_v02
from troute.nhd_io import read_custom_input


def generate_qlat(refactored_streams, orig_qlat):
    
    orig_qlat = orig_qlat.rename(columns={orig_qlat.columns[0]: 'ID'})
    # Convert time series columns to list
    qlat_ts_list = orig_qlat.to_numpy().tolist()
    # Create dict with ID: list of qlat time series
    qlat_dict_b = dict((str(int(l[0])), l[1:]) for l in qlat_ts_list)
    
    refact_qlat_dict = {}
    fraction_dict = {}
    orig_ids_missing_qlat = []
    
    # Convert crosswalk to dict
    refactored_streams_subset = refactored_streams[['link', 'lengthMap']]
    refactored_streams_subset.set_index('link',inplace=True)
    refactored_streams_dict = refactored_streams_subset.to_dict('index')
    
    del refactored_streams, refactored_streams_subset

    for newID, crosswalk in refactored_streams_dict.items():
        newID = str(newID)

        xwalk_list = crosswalk['lengthMap'].split(',')
        
        # Segments with multiple crosswalk IDs
        if len(xwalk_list) > 1:
        
            for i in xwalk_list:

                if '.' in i:
                    # Get ID and fraction of crosswalk
                    i, fraction = i.split('.')
                    
                    if fraction == '1':
                        fraction = 1
                    else:
                        fraction = float(fraction)/1000

                    if i in fraction_dict.keys():
                        fraction_dict[i] += fraction
                    else:
                        fraction_dict[i] = fraction
                    
                if i not in qlat_dict_b.keys():
                    orig_ids_missing_qlat = orig_ids_missing_qlat + [i]
                
                else:
                    if newID in refact_qlat_dict.keys():
                        qlat_sum = [sum(x) for x in zip(refact_qlat_dict[newID], list(map(lambda x:fraction*x, qlat_dict_b[i])))]
                        refact_qlat_dict[newID] = qlat_sum
                    else:
                        refact_qlat_dict[newID] = list(map(lambda x:fraction*x, qlat_dict_b[i]))
                
        # Segments with single crosswalk ID
        elif len(xwalk_list) == 1:

            if '.' in xwalk_list[0]:
                    # Get ID and fraction of crosswalk
                    i, fraction = xwalk_list[0].split('.')
                    
                    if fraction == '1':
                        fraction = 1
                    else:
                        fraction = float(fraction)/1000

                    if i in fraction_dict.keys():
                        fraction_dict[i] += fraction
                    else:
                        fraction_dict[i] = fraction
            else:
                i = xwalk_list[0]

            # Copy channel properties            
            if i not in qlat_dict_b.keys():
                orig_ids_missing_qlat = orig_ids_missing_qlat + [i]

            else:
                if newID in refact_qlat_dict.keys():
                    qlat_sum = [sum(x) for x in zip(refact_qlat_dict[newID], list(map(lambda x:fraction*x, qlat_dict_b[i])))]
                    refact_qlat_dict[newID] = qlat_sum
                                        
                else:
                    refact_qlat_dict[newID] = list(map(lambda x:fraction*x, qlat_dict_b[i]))
        else:
            print (f"no crosswalk for refactored segment {newID}")
     
    dict_to_df = pd.DataFrame(refact_qlat_dict.items(), columns=['ID', 'qlat'])
    
    refactored_qlat = pd.DataFrame(dict_to_df["qlat"].to_list(), columns=orig_qlat.columns[1:].to_list(),index= dict_to_df.ID)
    refactored_qlat.index.name = None 
    # refactored_qlat.reset_index()
    
    return refactored_qlat, orig_ids_missing_qlat, fraction_dict # , orig_id_set


def recalculate_flows(new_network_flows, qlat_sample_path):
    # generate Q time series for NWM v2.1 network IDs from troute
    print ('coming soon')


def convert_results_to_df(results, nts, return_courant = False):
    courant = pd.DataFrame()

    qvd_columns = pd.MultiIndex.from_product(
        [range(nts), ["q", "v", "d"]]
    ).to_flat_index()

    flowveldepth = pd.concat(
        [pd.DataFrame(r[1], index=r[0], columns=qvd_columns) for r in results],
        copy=False,
    )

    courant_columns = pd.MultiIndex.from_product(
        [range(nts), ["cn", "ck", "X"]]
    ).to_flat_index()

    courant = pd.concat(
        [
            pd.DataFrame(r[2], index=r[0], columns=courant_columns)
            for r in results
        ],
        copy=False,
    )

    return flowveldepth, courant


def run_troute_from_script(custom_input_file):
    (
        supernetwork_parameters,
        waterbody_parameters,
        forcing_parameters,
        restart_parameters,
        output_parameters,
        run_parameters,
        parity_parameters,
        data_assimilation_parameters,
        diffusive_parameters,
        coastal_parameters,
    ) = read_custom_input(custom_input_file)

    # Modify any parameters from a template file inline here
    # run_parameters['nts'] = nts

    # Then run the model and get the results
    results = _run_everything_v02(
            supernetwork_parameters,
            waterbody_parameters,
            forcing_parameters,
            restart_parameters,
            output_parameters,
            run_parameters,
            parity_parameters,
            data_assimilation_parameters,
            diffusive_parameters,
            coastal_parameters,
    )

    return results


def add_routelink_attributes(stream_layer, routlink_netcdf_filename):

    # Read CONUS routelink NetCDF
    routlink_netcdf = xr.open_dataset(routlink_netcdf_filename)
    routlink_df = routlink_netcdf.to_dataframe()
        
    # Add missing attributes
    stream_layer["NHDWaterbodyComID"] = None
    stream_layer["alt"] = None
    fraction_dict = {}
    ids_missing_parameters = []
    comid_set = set()
    for index, segment in stream_layer.iterrows():

        original_segment = segment.lengthMap.split(',')

        # Segments with multiple crosswalk IDs
        if len(original_segment) > 1:

            temp_segs = pd.DataFrame()
            for i in original_segment:

                if '.' in i:
                    # Get ID and fraction of crosswalk
                    i, fraction = i.split('.')

                    if i in fraction_dict.keys():
                        fraction_dict[i] += int(fraction)/10
                    else:
                        fraction_dict[i] = int(fraction)/10

                comid_set.add(i)
                temp_seg = routlink_df.loc[routlink_df.link == int(i)].copy()
                temp_segs = temp_segs.append(temp_seg)

                # Average channel properties
                stream_layer.loc[stream_layer.ID == segment.ID,"NHDWaterbodyComID"] = str(temp_segs['NHDWaterbodyComID'].max())
                stream_layer.loc[stream_layer.ID == segment.ID,"alt"] = np.round(temp_segs['alt'].mean(),2)

        # Segments with single crosswalk ID
        elif len(original_segment) == 1:

            if '.' in original_segment[0]:
                    # Get ID and fraction of crosswalk
                    i, fraction = original_segment[0].split('.')

                    if i in fraction_dict.keys():
                        fraction_dict[i] += int(fraction)/10
                    else:
                        fraction_dict[i] = int(fraction)/10
            else:
                i = original_segment[0]

            comid_set.add(i)
            # Copy channel properties
            temp_seg = routlink_df.loc[routlink_df.link == int(i)]

            if temp_seg.empty:
                print(f"segment ID {int(i)} is not in routelink file")
                ids_missing_parameters = ids_missing_parameters + [i]

            else:
                stream_layer.loc[stream_layer.ID == segment.ID,"NHDWaterbodyComID"] = str(temp_seg['NHDWaterbodyComID'].item())
                stream_layer.loc[stream_layer.ID == segment.ID,"alt"] = temp_seg['alt'].item()
        else:
            print (f"no crosswalk for segment {segment.ID}")

    return stream_layer, ids_missing_parameters, comid_set, fraction_dict


def split_routelink_waterbodies(stream_layer):

    for index, segment in stream_layer.iterrows():

        original_segment = segment.NHDWaterbodyComID.split(',')

        # Segments with multiple NHDWaterbodyCom IDs
        if len(original_segment) > 1:
            # Use first waterbody
            stream_layer.loc[stream_layer.link == segment.link,"NHDWaterbodyComID"] = original_segment[0]


    return stream_layer


def clean_dataset(original_df):

    # Add column
    original_df.index.name = 'COMID'
    original_df = original_df.reset_index()

    # Transpose dataframe
    original_df_melt = pd.melt(original_df, id_vars=['COMID'], var_name='parameters', value_name='values')

    # Unpack parameter column
    original_df_melt[['routing_timestep', 'routing_variable']] = pd.DataFrame(original_df_melt['parameters'].tolist(), index=original_df_melt.index)

    if original_df_melt.routing_timestep.dtype != 'int': original_df_melt.routing_timestep = original_df_melt.routing_timestep.astype(int)

    original_df_melt = original_df_melt.drop(columns=['parameters'])
    original_df_melt = original_df_melt[original_df_melt['routing_variable'] == 'cn']

    return original_df_melt


def replace_line(filename, dictionary, new_filename):

    with open(filename,"r") as f:
        get_all=f.readlines()

    with open(new_filename,'w') as f:
        
        for i,line in enumerate(get_all,1):
            
            if ':' in line:
                
                try:
                    att, var = line.split(':')
                except:
                    att, var, comment = line.split(':')
                
                att = att.strip()
                
                if att in dictionary.keys():
                    f.writelines(dictionary[att])
                else:
                    f.writelines(line)
            else:
                f.writelines(line)


def create_cn_summary_table(courant_results, stream_network, network):

    bad_segments_list = courant_results[courant_results['values']>= 1].COMID.unique()
    violating_segs = stream_network.loc[stream_network.link.isin(bad_segments_list)]

    cn_summary_table = pd.DataFrame({
                                    'Network': [network],
                                    'Min Segment Length (m)': np.round(stream_network.Length.min(),2),
                                    'Max Segment Length (m)': np.round(stream_network.Length.max(),2),
                                    'Avg Segment Length (m)': np.round(stream_network.Length.mean(),2),
                                    'Total Segment Length (m)': np.round(stream_network.Length.sum(),2),
                                    'Total Segments': stream_network.Length.count(),
                                    'Total Timesteps': len(courant_results.routing_timestep.unique()),
                                    'CN Violations': len(courant_results[courant_results['values']>= 1]),
                                    'Number of Segments Violating CN': len(bad_segments_list),
                                    'Length of Network Violating CN': violating_segs.Length.sum(),
                                    '% Segments Violating CN': np.round(100*(len(bad_segments_list)/stream_network.Length.count()),2)
                                    })

    return cn_summary_table