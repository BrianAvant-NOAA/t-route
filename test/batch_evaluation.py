#!/usr/bin/env python3

import argparse
from pathlib import Path
import os
import sys
from os.path import join
from concurrent.futures import ProcessPoolExecutor
from refactor_to_t_route import run_troute
import warnings
warnings.simplefilter(action='ignore', category=RuntimeWarning)


def batch_evaluation(nts,testing_dir,qlat_type,workers):

    testing_dir = Path(testing_dir)
    refactored_streams_dir = testing_dir / 'inputs' / "refactored"
    huc_ids = os.listdir(refactored_streams_dir)

    # Open log file
    sys.__stdout__ = sys.stdout
    log_file = open(join(str(testing_dir / 'run_troute.log')),"w")
    sys.stdout = log_file

    print (f"Running t-route on {len(huc_ids)} HUCs")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Run t-route on refactored networks
        troute_results = [executor.submit(run_troute,nts,testing_dir,qlat_type, str(huc)) for huc in huc_ids]

    # Close log file
    sys.stdout = sys.__stdout__
    log_file.close()

if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Run t-route routing algorithms and calculate metrics')
    parser.add_argument('-nts', '--nts', help='number of timesteps',required=True,type=int)
    parser.add_argument('-d', '--testing-dir', help='testing directory',required=True)
    parser.add_argument('-qlat_type', '--qlat-type', help='q_lat data type (timeseries or max)',required=False,default='ts')
    parser.add_argument('-workers', '--workers', help='Number of jobs to run',required=False,default=1,type=int)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    batch_evaluation(**args)
