#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: glosat-c3s-crutem-converter.py
#-----------------------------------------------------------------------
# Version 0.1
# 18 May, 2021
# Dr Michael Taylor
# https://patternizer.github.io
# patternizer AT gmail DOT com
# michael DOT a DOT taylor AT uea DOT ac DOT uk
#-----------------------------------------------------------------------

# Dataframe libraries:
import numpy as np
import pandas as pd
import xarray as xr
import pickle
from datetime import datetime

# I/O libraries:
import os, glob

# Plotting libraries:
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

fontsize = 16
data_in = 'DATA/C3S_csv/*.psv'
output_dir = 'OUT'
extract_rainfall = False
        
#-----------------------------------------------------------------------------
# EXTRACT: station to CRUTEM format
#-----------------------------------------------------------------------------

# Datasets (via Simon Noone):

#-----------------------------------------------------------------------------
# UNIX: convert pipe-separated values (PSV) to comma-separated values (CSV):
#-----------------------------------------------------------------------------
# sed -i "s%|%,%g" *
#-----------------------------------------------------------------------------

#ndex(['observation_id', 'report_type', 'date_time', 'date_time_meaning',
#      'latitude', 'longitude', 'observation_height_above_station_surface',
#      'observed_variable', 'units', 'observation_value', 'value_significance',
#      'observation_duration', 'platform_type', 'station_type',
#      'primary_station_id', 'station_name', 'quality_flag',
#      'data_policy_licence', 'source_id '],
#     dtype='object')

filelist = sorted(glob.glob(data_in))

for file in filelist:

    df = pd.read_csv(file, header=0)
    if extract_rainfall == True:
        dg = df[(df['observed_variable']==44)]
    else:
        dg = df[(df['observed_variable']==85) & (df['quality_flag']==3)]
    
    if len(dg) == 0:
        continue    

    if extract_rainfall == True:
        observations = dg['observation_value']
    else:
        observations = dg['observation_value'] - 273.15
    datetimes = dg['date_time'].astype('datetime64[ns]')
    dh = pd.DataFrame({'date':datetimes, 'measurement':observations}).reset_index(drop=True)    
    x = np.array(dh.groupby(dh['date'].dt.year), dtype='object')
    station_data = []
    for i in range(len(x)):
        yyyy = x[i][0]
        obs = np.ones(12)*np.nan
        for j in range(len(x[i][1])):
            month = x[i][1].date.iloc[j].month
            val = x[i][1].measurement.iloc[j]         
            obs[month-1] = val   
        yeardata = list([yyyy])+list(obs.ravel())
        station_data.append(yeardata)
        
    station_lat = dg['latitude'].unique()
    station_lon = dg['longitude'].unique()
    station_elevation = np.nan  
    station_name = dg['station_name'].unique()[0]
    station_country = str(np.nan)
    station_first = datetimes.dt.year.iloc[0]
    station_last = datetimes.dt.year.iloc[-1]
    station_source = 'XX'
    station_firstreliable = datetimes.dt.year.iloc[0]
    station_crucode = 'XXXXX'

    station_code_in = str(dg['primary_station_id'].unique()[0])
    if station_code_in[-6:][0] == '0':
        if station_code_in[-6:][1] == '6':
    	    station_code = station_code_in[-5:] + '0'
        elif station_code_in[-6:][1] == '0': 
    	    station_code = '6' + station_code_in[-4:] + '0'
        else:
    	    station_code = station_code_in	        
    else:
        station_code = station_code_in	        
    station_source = dg['source_id '].unique()

    # CRUTEM station header

    stationcode = "{:>6}".format(station_code[-6:])
    stationlat = "{:>4}".format(str(int(station_lat*10)))
    stationlon = "{:>5}".format(str(int(station_lon*10)))
    stationelevation = "{:>5}".format(str(station_elevation))
    stationname = "{:<20}".format(station_name[:20])
    stationcountry = "{:<13}".format(station_country[:13])
    stationfirst = "{:>4}".format(str(station_first))
    stationlast = "{:>4}".format(str(station_last))
    stationsource = "{:>2}".format('XX')    
    stationfirstreliable = "{:>4}".format(str(station_firstreliable))
    stationcrucode = "{:>8}".format('XXXXX')
    
    station_header = stationcode + stationlat + stationlon + stationelevation + ' ' + stationname + ' ' + stationcountry + ' ' + stationfirst + stationlast + '  ' + stationsource + stationfirstreliable + stationcrucode
    
    #------------------------------------------------------------------------------
    # WRITE: station header + yearly rows of monthly values in CRUTEM format
    #------------------------------------------------------------------------------

    if extract_rainfall == True:
        stationfile_c3s = output_dir + '/' + station_code + '_' + 'crutem_format' + '_' + 'rainfall' + '.txt'
    else:
        stationfile_c3s = output_dir + '/' + station_code + '_' + 'crutem_format' + '_' + 'temperature' + '.txt'

    with open(stationfile_c3s,'w') as g:
        g.write(station_header+'\n')
        for i in range(len(station_data)):  
            year = str(int(station_data[i][0]))
            rowstr = year
            for j in range(1,13):
                if np.isnan(station_data[i][j]):
                    monthstr = str(-999)
                else:
                    monthstr = str(int(station_data[i][j]*10))
                rowstr += f"{monthstr:>5}"          
            g.write(rowstr+'\n')
    g.close()       
                    
#-----------------------------------------------------------------------------
print('** END')
