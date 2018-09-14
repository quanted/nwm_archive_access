import os
import sys
import time
import csv
import traceback
import numpy as np
import netCDF4
import requests


def read_nmw_files(nc_file):
    
    try:
        rootgrp = netCDF4.Dataset(nc_file, 'r')

        metadata_dims = ['feature_id']
        metadata_dim = len(rootgrp.dimensions[metadata_dims[0]])
        print(rootgrp.groups)

        v_feature_id = rootgrp.variables['feature_id'][:]
        v_q_lateral = rootgrp.variables['q_lateral'][:]        
        v_qBtmVertRunoff = rootgrp.variables['qBtmVertRunoff'][:]
        v_qBucket = rootgrp.variables['qBucket'][:]
        v_qSfcLatRunoff = rootgrp.variables['qSfcLatRunoff'][:]
        v_streamflow = rootgrp.variables['streamflow'][:]
        v_velocity = rootgrp.variables['velocity'][:]
        print(len(v_feature_id))
        print(len(v_q_lateral))
        print(len(v_qBtmVertRunoff))
        print(len(v_qBucket))
        print(len(v_qSfcLatRunoff))
        print(len(v_streamflow))
        print(len(v_velocity))


        rootgrp.close()

    except Exception as e:
        print(e)



if __name__ == '__main__':


    #This script downmloads a specified collection of National Water Model historical data NetCDF files.
    #It is currently hard coded to retrieve July 2014 hourly Streamflow values at points associated with flow lines - CHRTOUT data.
    #https://docs.opendata.aws/nwm-archive/readme.html
    #https://registry.opendata.aws/nwm-archive/

    #This script generates the datetime values and filenames and writes to a file.
    #Use the read_netcdf_files script.py to extract the data once the files are downloaded.

    #'https://nwm-archive.s3.amazonaws.com/2017/201701011500.CHRTOUT_DOMAIN1.comp'

    year = '2014'
    month = '07'
    file_name_template = '{0}{1}{2}{3}00.CHRTOUT_DOMAIN1.comp'

    base_url = 'https://nwm-archive.s3.amazonaws.com/{0}/'
    
    nwm_files = list()
    try:

        for day in range(1,32):
            for hour in range(0,24):
                #time.sleep(0.5)
                #print year + "  " + month + "  " + str(day) + "   " + str(hour)
                row = list()
                url = base_url.format(year)
                file_name = file_name_template.format(year, month, str(day).zfill(2), str(hour).zfill(2))
                row.append(file_name)
                row.append(year)
                row.append(month)
                row.append(str(day).zfill(2))
                row.append(str(hour).zfill(2))
                nwm_files.append(row)

        with open('nwm_files.csv', 'w') as f:
            writer = csv.writer(f, lineterminator = '\n')
            writer.writerows(nwm_files)
            

        time.sleep(2.0)                

        nwm_files = list()
        with open('nwm_files.csv') as f:
            #nwm_files = [line.rstrip() for line in f]
            reader = csv.reader(f)
            for row in reader:
                nwm_files.append(row)

            


        for nwm_file in nwm_files:

                write_file = os.path.join("nwm_output", nwm_file)

                #Dont need to get the file again if it exists on disk
                if os.path.isfile(write_file):
                    continue

                get_url = url + nwm_file
                resp = requests.get(get_url, allow_redirects=True)
                
                with open(write_file, 'wb') as f:
                    f.write(resp.content)

    except Exception as e:
        print(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exc(file=sys.stdout)
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)    
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)


    # year = '2017'
    # month = '01'
    # day = '01'
    # time = '0000'
    # file_template  = "{}{}{}{}".format(year, month, day, time)
    # file = "{}.CHRTOUT_DOMAIN1.nc".format(file_template)
    # nc_file = os.path.join('input', file)
