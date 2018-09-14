import os
import sys
import time
import datetime
import csv
import logging
import traceback
import netCDF4
import numpy as np
import requests

#Found this link useful for working with netCDF files
#http://schubert.atmos.colostate.edu/~cslocum/netcdf_example.html

def read_nmw_files(nc_file, comids, date_time):
    
    try:
        
        rootgrp = netCDF4.Dataset(nc_file, 'r')

        v_feature_id = rootgrp.variables['feature_id'][:]
        v_q_lateral = rootgrp.variables['q_lateral'][:]        
        v_qBtmVertRunoff = rootgrp.variables['qBtmVertRunoff'][:]
        v_qBucket = rootgrp.variables['qBucket'][:]
        v_qSfcLatRunoff = rootgrp.variables['qSfcLatRunoff'][:]
        v_streamflow = rootgrp.variables['streamflow'][:]
        v_velocity = rootgrp.variables['velocity'][:]

        data = list()
        for comid in comids:
            row = list()

            #dt = date_time.strftime('%Y%m%d%H')
            #row.append(dt)
            row.append(date_time.strftime('%Y%m%d%H') + '00')
            row.append(comid)
            idx = np.where(v_feature_id == comid)

            val = v_streamflow[idx][0]
            
            row.append(v_q_lateral[idx][0])
            row.append(v_qBtmVertRunoff[idx][0])
            row.append(v_qBucket[idx][0])
            row.append(v_qSfcLatRunoff[idx][0])
            row.append(v_streamflow[idx][0])
            row.append(v_velocity[idx][0])
            data.append(row)
        
        return data
        
    except Exception as e:
        print(e)
        logging.debug("File: " + nc_file)

    finally:
        v = 1
        #rootgrp.close()


if __name__ == '__main__':

    logging.basicConfig(filename="validate_stream_width.log",level=logging.DEBUG)
    try:

        nwm_files = list()
        with open('nwm_files.csv') as f:
            reader = csv.reader(f)
            for row in reader:
                nwm_files.append(row)

        comids = list()
        with open('comids_030502040102_2.csv') as f:
            comids = [int(line.rstrip()) for line in f]
            #reader = csv.reader(f)
            #for row in reader:
            #    comids.append(row)

        data = list()
        i = 1
        for nwm_file in nwm_files:

            read_file = os.path.join("nwm_output", nwm_file[0])

            #Dont need to get the file again if it exists on disk
            if os.path.isfile(read_file) == False:
                continue

            date_time = datetime.datetime(int(nwm_file[1]), int(nwm_file[2]), int(nwm_file[3]), int(nwm_file[4]))
            
            rows = read_nmw_files(read_file, comids, date_time)
            for row in rows:
                data.append(row)

            logging.debug("Row: " + str(i))
            i = i + 1


        with open('nwm_030502040102_times.csv', 'w') as f:
            writer = csv.writer(f, lineterminator = '\n')
            writer.writerows(data)

    except Exception as e:
        print(e)
        logging.debug(str(e))
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
