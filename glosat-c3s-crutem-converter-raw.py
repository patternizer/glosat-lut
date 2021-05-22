#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: glosat-c3s-crutem-converter-raw.py
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
import re

# I/O libraries:
import os, glob

# Plotting libraries:
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

fontsize = 16
input_dir = 'DATA/C3S_raw'
output_dir = 'OUT'

file_tmin = 'fwa.min'
file_tmax = 'fwa.max'

#file_tmin = 'griffiths-sa.min'
#file_tmax = 'griffiths-sa.max'

#file_tmin = 'south-africa.min'
#file_tmax = 'south-africa.max'

#file_tmin = 'wwr.min'
#file_tmax = 'wwr.max'

#file_tmin = 'ghcn.min'
#file_tmax = 'ghcn.max'

#file_tmin = 'ghcnd-nonconus.min'
#file_tmax = 'ghcnd-nonconus.max'

#-----------------------------------------------------------------------------
# EXTRACT: station to CRUTEM format
#-----------------------------------------------------------------------------

# Datasets (via Simon Noone):

#The files are in a header/data format.

#The content of the header lines is ID, NAME, LATITUDE, LONGITUDE, 
#ELEVATION, FIRSTYEAR, LASTYEAR, COUNTRY, SOURCECODE 
#(a12,a30,i5,i6,i4,i4,i4,a20,a10).  You can ignore the SOURCECODE, and 
#COUNTRY may or may not be accurate.  Missing LATITUDES are -9999, 
#LONGITUDES are -99999, and ELEVATIONS are -999.

#The content of the data lines is YEAR, JAN, FEB, ..., DEC (i4,12i5).  
#Missing values are -9999.

# C3S: (a12,a30,i5,i6,i4,i4,i4,a20,a10)
# 10300005306 KANDI                          1113   293 29019451979BENIN               MET FRANCE

# CONSTRUCT: dataframe of Tmin

file = input_dir + '/' + file_tmin

nheader = 0
years = []
obs = []
station_id = []
station_lat = []
station_lon = []
station_elevation = []
station_name = []
station_country = []
station_firstyear = []
station_lastyear = []
station_sourcecode = []
station_firstreliable = []

f = open(file)
lines = f.readlines()
for i in range(nheader,len(lines)):    
#    if (lines[i][4] == '-') | (lines[i][4].isspace()):        
    if (~lines[i][0].isspace()) & ((lines[i][4] == '-') | (lines[i][4].isspace())):        
        year = int(lines[i][0:4])                   
        val = 12*[None]
        for j in range(len(val)):
            val[j] = float(lines[i][4+(j*5):4+(j+1)*5])
        years.append(year)
        obs.append(val) 

        station_id.append( ident )                  # C3S=a12,  CRU=i6
        station_lat.append( lat )                   # C3S=i5,   CRU=i4
        station_lon.append( lon )                   # C3S=i6,   CRU=i5
        station_elevation.append( elevation )       # C3S=i4,   CRU=i5
        station_name.append( name )                 # C3S=a30,  CRU=a20
        station_country.append( country )           # C3S=a20,  CRU=a13
        station_firstyear.append( firstyear )       # C3S=i4,   CRU=i4
        station_lastyear.append( lastyear )         # C3S=i4,   CRU=i4
        station_sourcecode.append( sourcecode )     # C3S=a10,  CRU=a8
        station_firstreliable.append( firstyear )   # C3S=i4,   CRU=i4

    else:
        ident = lines[i][0:12].rstrip()
#        lat = int(lines[i][42:48].strip())
#        lon = int(lines[i][48:53].strip()) 
        lat = int( np.round( float(lines[i][42:47].strip())/10.0,0) ) # round 2 d.p. to 1 d.p.
        lon = int( np.round( float(lines[i][47:53].strip())/10.0,0) ) # round 2 d.p. to 1 d.p.
        elevation = int(lines[i][53:57].strip())
        name = lines[i][12:42].strip()
        country = lines[i][65:85].strip()
        firstyear = int(lines[i][57:61].strip())
        lastyear = int(lines[i][61:65].strip())
        sourcecode = lines[i][85:].strip() 
        firstreliable = firstyear
        
f.close()

# construct dataframe
    
df = pd.DataFrame(columns=['year','1','2','3','4','5','6','7','8','9','10','11','12'])
df['year'] = years

for j in range(1,13):
    df[df.columns[j]] = [ obs[i][j-1] for i in range(len(obs)) ]

df['stationcode'] = station_id
df['stationlat'] = station_lat
df['stationlon'] = station_lon
df['stationelevation'] = station_elevation
df['stationname'] = station_name
df['stationcountry'] = station_country
df['stationfirstyear'] = station_firstyear
df['stationlastyear'] = station_lastyear
df['stationsource'] = station_sourcecode
df['stationfirstreliable'] = station_firstreliable
    
# CONSTRUCT: dataframe of Tmax

file = input_dir + '/' + file_tmax

nheader = 0
years = []
obs = []
station_id = []
station_lat = []
station_lon = []
station_elevation = []
station_name = []
station_country = []
station_firstyear = []
station_lastyear = []
station_sourcecode = []
station_firstreliable = []
    
f = open(file)
lines = f.readlines()
for i in range(nheader,len(lines)):    
#   if (lines[i][4] == '-') | (lines[i][4].isspace()):        
    if (~lines[i][0].isspace()) & ((lines[i][4] == '-') | (lines[i][4].isspace())):        
        year = int(lines[i][0:4])                   
        val = 12*[None]
        for j in range(len(val)):
            val[j] = float(lines[i][4+(j*5):4+(j+1)*5])
        years.append(year)
        obs.append(val) 

        station_id.append( ident )                  # C3S=a12,  CRU=i6
        station_lat.append( lat )                   # C3S=i5,   CRU=i4
        station_lon.append( lon )                   # C3S=i6,   CRU=i5
        station_elevation.append( elevation )       # C3S=i4,   CRU=i5
        station_name.append( name )                 # C3S=a30,  CRU=a20
        station_country.append( country )           # C3S=a20,  CRU=a13
        station_firstyear.append( firstyear )       # C3S=i4,   CRU=i4
        station_lastyear.append( lastyear )         # C3S=i4,   CRU=i4
        station_sourcecode.append( sourcecode )     # C3S=a10,  CRU=a8
        station_firstreliable.append( firstyear )   # C3S=i4,   CRU=i4

    else:
        ident = lines[i][0:12].rstrip()
#        lat = int(lines[i][42:48].strip())
#        lon = int(lines[i][48:53].strip()) 
        lat = int( np.round( float(lines[i][42:47].strip())/10.0,0) ) # round 2 d.p. to 1 d.p.
        lon = int( np.round( float(lines[i][47:53].strip())/10.0,0) ) # round 2 d.p. to 1 d.p.        
        elevation = int(lines[i][53:57].strip())
        name = lines[i][12:42].strip()
        country = lines[i][65:85].strip()
        firstyear = int(lines[i][57:61].strip())
        lastyear = int(lines[i][61:65].strip())
        sourcecode = lines[i][85:].strip() 
        firstreliable = firstyear
        
f.close()

# construct dataframe of tmin
    
dg = pd.DataFrame(columns=['year','1','2','3','4','5','6','7','8','9','10','11','12'])
dg['year'] = years

for j in range(1,13):
    dg[dg.columns[j]] = [ obs[i][j-1] for i in range(len(obs)) ]

dg['stationcode'] = station_id
dg['stationlat'] = station_lat
dg['stationlon'] = station_lon
dg['stationelevation'] = station_elevation
dg['stationname'] = station_name
dg['stationcountry'] = station_country
dg['stationfirstyear'] = station_firstyear
dg['stationlastyear'] = station_lastyear
dg['stationsource'] = station_sourcecode
dg['stationfirstreliable'] = station_firstreliable

# CONSTRUCT: dataframe of Tmean = (Tmin+Tmax)/2

dh = df.copy()

for j in range(1,13):    
    df[df.columns[j]].replace(-9999, np.nan, inplace=True)
    dg[dg.columns[j]].replace(-9999, np.nan, inplace=True)

for j in range(1,13):    
    dh[str(j)] = (df[str(j)]+dg[str(j)])/2

# REPLACE: fill_Values with NaN

for j in range(1,13):    
    dh[dh.columns[j]].replace(np.nan, -999, inplace=True)

# WRITE: station files in CRUTEM format
        
stationlist = dh['stationcode'].unique()
for i in range(len(stationlist)):
    stationdata = dh[dh['stationcode']==stationlist[i]]
                        
    #------------------------------------------------------------------------------
    # EXTRACT: station data + metadata
    #------------------------------------------------------------------------------
    
    station_data = dh[dh['stationcode']==stationlist[i]].iloc[:,range(0,13)].reset_index(drop=True)
    station_metadata = dh[dh['stationcode']==stationlist[i]].iloc[0,range(14,23)]

    #------------------------------------------------------------------------------
    # FORMAT: station header components in CRUTEM format
    #
    # 37401 525  -17  100 HadCET on 29-11-19   UK            16592019  351721     NAN
    #2019   40   66   78   91  111  141  175  171  143  100 -999 -999
    #------------------------------------------------------------------------------

    # CRU: (i6,i4,i5,i5,x,a20,x,a13,x,i4,i4,2x,i2,i4,a8)
    # 010010 709   87   10 Jan Mayen            NORWAY        19212019  561921   99950
    
    stationcode = "{:>6}".format('XXXXXX')
    stationlat = "{:>4}".format(str(int(station_metadata[0]*1)))
    stationlon = "{:>5}".format(str(int(station_metadata[1]*1)))
    stationelevation = "{:>5}".format(str(station_metadata[2]))
    stationname = "{:<20}".format(station_metadata[3][:20])
    stationcountry = "{:<13}".format(station_metadata[4][:13])
    stationfirst = "{:>4}".format(str(int(station_metadata[5]*1)))
    stationlast = "{:>4}".format(str(int(station_metadata[6]*1)))
    stationsource = "{:>2}".format('XX')    
    stationfirstreliable = "{:>4}".format(str(station_metadata[8]))
    stationcrucode = "{:>8}".format('XXXXX')
    
    station_header = stationcode + stationlat + stationlon + stationelevation + ' ' + stationname + ' ' + stationcountry + ' ' + stationfirst + stationlast + '  ' + stationsource + stationfirstreliable + stationcrucode
    
    #------------------------------------------------------------------------------
    # WRITE: station header + yearly rows of monthly values in CRUTEM format
    #------------------------------------------------------------------------------
    
    stationfile = output_dir + '/' + stationlist[i] + '.txt'
    with open(stationfile,'w') as f:
        f.write(station_header+'\n')
        for i in range(len(station_data)):  
            year = str(int(station_data.iloc[i,:][0]))
            rowstr = year
            for j in range(1,13):
                if np.isnan(station_data.iloc[i,:][j]):
                    monthstr = str(-999)
                else:
                    monthstr = str(int(station_data.iloc[i,:][j]*1))
                rowstr += f"{monthstr:>5}"          
            f.write(rowstr+'\n')
                
#-----------------------------------------------------------------------------
print('** END')
