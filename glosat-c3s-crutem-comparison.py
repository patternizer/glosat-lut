#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: glosat-c3s-crutem-comparison.py
#-----------------------------------------------------------------------
# Version 0.2
# 19 May, 2021
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

# Stats libraries:
import scipy.stats as stats
import statsmodels.api as sm

#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

fontsize = 16
data_dir = 'DATA/C3S_csv/*.psv'
output_dir = 'OUT'

one_station = True
pkl_archive = 'DATA/df_temp.pkl'
stationcode = '619670' # Diego Garcia

stationfile_cru = output_dir + '/' + stationcode + '_' + 'cru.txt'
stationfile_c3s = output_dir + '/' + stationcode + '_' + 'c3s.txt'

#------------------------------------------------------------------------------
# LOAD: GloSAT absolute temperature archive in pickled pandas dataframe format
#------------------------------------------------------------------------------

df_temp = pd.read_pickle(pkl_archive, compression='bz2')
try:
    value = np.where(df_temp['stationcode'].unique()==stationcode)[0][0]
except:        
    print('Stationcode not in archive')
    
#------------------------------------------------------------------------------
# EXTRACT: station data + metadata
#------------------------------------------------------------------------------

station_data = df_temp[df_temp['stationcode']==df_temp['stationcode'].unique()[value]].iloc[:,range(0,13)].reset_index(drop=True)
station_metadata = df_temp[df_temp['stationcode']==df_temp['stationcode'].unique()[value]].iloc[0,range(14,23)]

da = df_temp[df_temp['stationcode']==df_temp['stationcode'].unique()[value]]
ts_cru = np.array(da.iloc[:,1:13]).ravel()
t_cru = pd.date_range(start=str(da.year.iloc[0]), periods=len(ts_cru), freq='M')   
        
#------------------------------------------------------------------------------
# FORMAT: station header components in CRUTEM format
#
# 37401 525  -17  100 HadCET on 29-11-19   UK            16592019  351721     NAN
#2019   40   66   78   91  111  141  175  171  143  100 -999 -999
#------------------------------------------------------------------------------

stationlat = "{:<4}".format(str(int(station_metadata[0]*10)))
stationlon = "{:<4}".format(str(int(station_metadata[1]*10)))
stationelevation = "{:<3}".format(str(station_metadata[2]))
stationname = "{:<20}".format(station_metadata[3][:20])
stationcountry = "{:<13}".format(station_metadata[4][:13])
stationfirstlast = str(station_metadata[5]) + str(station_metadata[6])
stationsourcefirst = "{:<8}".format(str(station_metadata[7]) + str(station_metadata[8]))
stationgridcell = "{:<3}".format('NAN')
station_header = ' ' + stationcode[1:] + ' ' + stationlat + ' ' + stationlon + ' ' + stationelevation + ' ' + stationname + ' ' + stationcountry + ' ' + stationfirstlast + '  ' + stationsourcefirst + '   ' + stationgridcell 

#------------------------------------------------------------------------------
# WRITE: station header + yearly rows of monthly values in CRUTEM format
#------------------------------------------------------------------------------

with open(stationfile_cru,'w') as f:
    f.write(station_header+'\n')
    for i in range(len(station_data)):  
        year = str(int(station_data.iloc[i,:][0]))
        rowstr = year
        for j in range(1,13):
            if np.isnan(station_data.iloc[i,:][j]):
                monthstr = str(-999)
            else:
                monthstr = str(int(station_data.iloc[i,:][j]*10))
            rowstr += f"{monthstr:>5}"          
        f.write(rowstr+'\n')
f.close()
        
#-----------------------------------------------------------------------------
# EXTRACT: station to CRUTEM format
#-----------------------------------------------------------------------------

# Datasets (via Simon Noone):

#ndex(['observation_id', 'report_type', 'date_time', 'date_time_meaning',
#      'latitude', 'longitude', 'observation_height_above_station_surface',
#      'observed_variable', 'units', 'observation_value', 'value_significance',
#      'observation_duration', 'platform_type', 'station_type',
#      'primary_station_id', 'station_name', 'quality_flag',
#      'data_policy_licence', 'source_id '],
#     dtype='object')

filelist = sorted(glob.glob(data_dir))

if one_station == True:

    filelist = ['DATA/C3S_csv/CDM_lite_r202102_IOW00070701.psv']

    file = filelist[0]    
    df = pd.read_csv(file, header=0)
    dg = df[(df['observed_variable']==85) & (df['quality_flag']==3)]
    temperatures = dg['observation_value'] - 273.15
    datetimes = dg['date_time'].astype('datetime64[ns]')
    dh = pd.DataFrame({'date':datetimes, 'tmean':temperatures}).reset_index(drop=True)    
    x = np.array(dh.groupby(dh['date'].dt.year), dtype='object')
    station_data = []
    for i in range(len(x)):
        yyyy = x[i][0]
        obs = np.ones(12)*np.nan
        for j in range(len(x[i][1])):
            month = x[i][1].date.iloc[j].month
            val = x[i][1].tmean.iloc[j]         
            obs[month-1] = val   
        yeardata = list([yyyy])+list(obs.ravel())
        station_data.append(yeardata)
        
    station_lat = dg['latitude'].unique()
    station_lon = dg['longitude'].unique()
    station_elevation = np.nan  
    station_name = str(dg['station_name'].unique()[0])
    station_country = 'NAN'
    station_firstlast = str(datetimes.dt.year.iloc[0]) + str(datetimes.dt.year.iloc[-1])
    station_sourcefirst = 'XX' + str(datetimes.dt.year.iloc[0])
    station_gridcell = 'NAN'   
    station_code = str(dg['primary_station_id'].unique()[0])
    station_source = dg['source_id '].unique()

    # CRUTEM station header

    stationlat = "{:<4}".format(str(int(station_lat*10)))
    stationlon = "{:<4}".format(str(int(station_lon*10)))
    stationelevation = "{:<3}".format(str(station_elevation))
    stationname = "{:<20}".format(station_name[:20])
    stationcountry = "{:<13}".format(station_country[:13])
    stationfirstlast = station_firstlast
    stationsourcefirst = "{:<8}".format(station_sourcefirst)
    stationgridcell = "{:<3}".format(station_gridcell)    
    stationcode = "{:<6}".format(station_code[-5:])
    station_header = ' ' + stationcode + ' ' + stationlat + ' ' + stationlon + ' ' + stationelevation + ' ' + stationname + ' ' + stationcountry + ' ' + stationfirstlast + '  ' + stationsourcefirst + '   ' + stationgridcell 

    #------------------------------------------------------------------------------
    # WRITE: station header + yearly rows of monthly values in CRUTEM format
    #------------------------------------------------------------------------------

    with open(stationfile_c3s,'w') as g:
        g.write(station_header+'\n')
        for i in range(len(station_data)):  
            year = str(int(station_data[i][0]))
            print('year=',str(year))
            rowstr = year
            for j in range(1,13):
                if np.isnan(station_data[i][j]):
                    monthstr = str(-999)
                else:
                    monthstr = str(int(station_data[i][j]*10))
                rowstr += f"{monthstr:>5}"          
            g.write(rowstr+'\n')
    g.close()       

ts_c3s = np.array([ station_data[i][1:] for i in range(len(station_data)) ]).ravel()
t_c3s = pd.date_range(start=str(station_data[0][0]), periods=len(ts_c3s), freq='M')   
           
#-----------------------------------------------------------------------------
# PLOT: compare GloSAT v C3S
#-----------------------------------------------------------------------------

figstr = 'glosat-versus-c3s' + '-' + station_code + '.png'
titlestr = 'GloSAT.p03 v C3S QC monthly holdings: ' + stationname + '(' + station_code + ')'

fig,ax = plt.subplots(figsize=(15,10))
plt.plot(t_cru, ts_cru, color='purple', alpha=0.2, lw=1)
plt.plot(t_c3s, ts_c3s, color='teal', alpha=0.2, lw=1)
plt.plot(t_cru, pd.Series(ts_cru).rolling(window=12, center=True).mean(), color='purple', alpha=1.0, lw=3, label='GloSAT.p03: 1y MA')
plt.plot(t_c3s, pd.Series(ts_c3s).rolling(window=12, center=True).mean(), color='teal', alpha=1.0, lw=3, label='C3S QC: 1y MA')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
plt.tick_params(labelsize=fontsize)
plt.legend(loc='lower left', bbox_to_anchor=(0, -0.8), ncol=1, facecolor='lightgrey', framealpha=1, fontsize=fontsize)    
fig.subplots_adjust(left=None, bottom=0.4, right=None, top=None, wspace=None, hspace=None)             
plt.ylabel("Absolute temperature, $\mathrm{\degree}C$", fontsize=fontsize)
plt.title(titlestr, fontsize=fontsize)
plt.savefig(figstr, dpi=300)
plt.close('all')

#-----------------------------------------------------------------------------
print('** END')
