#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#------------------------------------------------------------------------------
# PROGRAM: plot-station-months-map.py
#------------------------------------------------------------------------------
# Version 0.1
# 22 April, 2020
# Michael Taylor
# https://patternizer.github.io
# patternizer AT gmail DOT com
# michael DOT a DOT taylor AT uea DOT ac DOT uk
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# IMPORT PYTHON LIBRARIES
#------------------------------------------------------------------------------
# Dataframe libraries:
import numpy as np
import numpy.ma as ma
from scipy.interpolate import griddata
from scipy import spatial
import itertools
import pandas as pd
import xarray as xr
#import klib
import pickle
from datetime import datetime
import nc_time_axis
import cftime
# Plotting libraries:
import matplotlib
import matplotlib.pyplot as plt; plt.close('all')
import matplotlib.cm as cm
from matplotlib import colors as mcol
from matplotlib.cm import ScalarMappable
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
from matplotlib.collections import PolyCollection
from mpl_toolkits import mplot3d
from mpl_toolkits.mplot3d import Axes3D
from mpl_toolkits.mplot3d import proj3d
import cmocean
# Mapping libraries:
import cartopy
import cartopy.crs as ccrs
from cartopy.io import shapereader
import cartopy.feature as cf
from cartopy.util import add_cyclic_point
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
# OS libraries:
import os
import os.path
from pathlib import Path
import sys
import subprocess
from subprocess import Popen
import time

# Silence library version notifications
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# SETTINGS: 
#------------------------------------------------------------------------------

fontsize = 20
load_df_temp = True    
load_df_anom = True
load_df_norm = True
load_df_normals = True
load_copernicus = True
plot_station_months_map_copernicus = True
plot_station_months_map_glosat = True
start_year = 1781
end_year = 1849

output_dir = 'OUT'

#------------------------------------------------------------------------------
# METHODS
#------------------------------------------------------------------------------

def load_dataframe(filename_txt):
    
    #------------------------------------------------------------------------------
    # I/O: filename.txt (text version)
    #------------------------------------------------------------------------------

    # load .txt file (comma separated) into pandas dataframe

#    filename_txt = 'stat4.GloSATprelim03.1658-2020.txt'
#    filename_txt = 'stat4.GloSATprelim02.1658-2020.txt'
#    filename_txt = 'stat4.CRUTEM5.1prelim01.1721-2019.txt'    
#    filename_txt = 'GloSATprelim01.1721-2019.txt'
    
    yearlist = []
    monthlist = []
    stationcode = []
    stationlat = []
    stationlon = []
    stationelevation = []
    stationname = []
    stationcountry = []
    stationfirstyear = []
    stationlastyear = []
    stationsource = []
    stationfirstreliable = []
    stationfirstreliable = []
    stationcruindex = []
    stationgridcell = []

    with open (filename_txt, 'r', encoding="ISO-8859-1") as f:  
                    
        for line in f:   
            if len(line)>1: # ignore empty lines         
                if (len(line.strip().split())!=13) | (len(line.split()[0])>4):   
                    # when line is station header extract info
                    #
                    # Station Header File:
                    #
                    #    (ch. 1-7) World Meteorological Organization (WMO) Station Number with a single additional character making a field of 6 integers. WMO numbers comprise a 5 digit sub-field, where the first two digits are the country code and the next three digits designated by the National Meteorological Service (NMS). Some country codes are not used. If the additional sixth digit is a zero, then the WMO number is or was an official WMO number. If the sixth digit is not zero then the station does not have an official WMO number and an alternative number has been assigned by CRU. Two examples are given below. Many additional stations are grouped beginning 99****. Station numbers in the blocks 72**** to 75**** are additional stations in the United States.
                    #    (ch. 8-11) Station latitude in degrees and tenths (-999 is missing), with negative values in the Southern Hemisphere
                    #    (ch. 12-16) Station longitude in degrees and tenths (-1999 is missing), with negative values in the Eastern Hemisphere (NB this is opposite to the more usual convention)
                    #    (ch. 18-21) Station Elevation in metres (-999 is missing)
                    #    (ch. 23-42) Station Name
                    #    (ch. 44-56) Country
                    #    (ch. 58-61) First year of monthly temperature data
                    #    (ch. 62-65) Last year of monthly temperature data
                    #    (ch. 68-69) Data Source (see below)
                    #    (ch. 70-73) First reliable year (generally the same as the first year)
                    #    (ch. 75-78) Unique index number (internal use)
                    #    (ch. 80-83) Index into the 5° x 5° gridcells (internal use) 
                    code = line[0:6]                    
                    lat = line[6:10]
                    lon = line[10:15]
                    elevation = line[17:20]
                    name = line[21:41]
                    country = line[42:55]
                    firstyear = line[56:60]
                    lastyear = line[60:64]
                    source = line[64:68]
                    firstreliable = line[68:72]
                    cruindex = line[72:77]
                    gridcell = line[77:80]                                                    
                else:           
                    yearlist.append(int(line.strip().split()[0]))                               
                    monthlist.append(np.array(line.strip().split()[1:]))                                 
                    stationcode.append(code)
                    stationlat.append(lat)
                    stationlon.append(lon)
                    stationelevation.append(elevation)
                    stationname.append(name)
                    stationcountry.append(country)
                    stationfirstyear.append(firstyear)
                    stationlastyear.append(lastyear)
                    stationsource.append(source)
                    stationfirstreliable.append(firstreliable)
                    stationcruindex.append(cruindex)
                    stationgridcell.append(gridcell)
            else:
                continue
    f.close

    # construct dataframe
    
    df = pd.DataFrame(columns=['year','1','2','3','4','5','6','7','8','9','10','11','12'])
    df['year'] = yearlist

    for j in range(1,13):
        df[df.columns[j]] = [ monthlist[i][j-1] for i in range(len(monthlist)) ]

    df['stationcode'] = stationcode
    df['stationlat'] = stationlat
    df['stationlon'] = stationlon
    df['stationelevation'] = stationelevation
    df['stationname'] = stationname
    df['stationcountry'] = stationcountry
    df['stationfirstyear'] = stationfirstyear
    df['stationlastyear'] = stationlastyear
    df['stationsource'] = stationsource
    df['stationfirstreliable'] = stationfirstreliable
#    df['stationcruindex'] = stationcruindex
#    df['stationgridcell'] = stationgridcell

    # trim strings
    
    df['stationname'] = [ str(df['stationname'][i]).strip() for i in range(len(df)) ] 
    df['stationcountry'] = [ str(df['stationcountry'][i]).strip() for i in range(len(df)) ] 

    # convert numeric types to int (important due to fillValue)

    for j in range(1,13):
        df[df.columns[j]] = df[df.columns[j]].astype('int')

    df['stationlat'] = df['stationlat'].astype('int')
    df['stationlon'] = df['stationlon'].astype('int')
    df['stationelevation'] = df['stationelevation'].astype('int')
    df['stationfirstreliable'] = df['stationfirstreliable'].astype('int')

    # error handling

#    for i in range(len(df)):        
#        if str(df['stationcruindex'][i])[1:].isdigit() == False:
#            df['stationcruindex'][i] = np.nan
#        else:
#            continue
 
    # replace fill values in int variables

    # -999 for stationlat
    # -9999 for stationlon
    # -9999 for station elevation
    # (some 999's occur elsewhere - fill all bad numeric cases with NaN)

    for j in range(1,13):    
        df[df.columns[j]].replace(-999, np.nan, inplace=True)

    df['stationlat'].replace(-999, np.nan, inplace=True) 
    df['stationlon'].replace(-9999, np.nan, inplace=True) 
    df['stationelevation'].replace(-9999, np.nan, inplace=True) 
#   df['stationfirstreliable'].replace(8888, np.nan, inplace=True)      
    
    return df

#------------------------------------------------------------------------------
# LOAD: absolute temperatures
#------------------------------------------------------------------------------

if load_df_temp == True:

    print('loading temperatures ...')

    df_temp = pd.read_pickle('DATA/df_temp.pkl', compression='bz2')    
    
    # EXTRACT TARBALL: IF df.csv IS COMPRESSED:

#   filename = Path("df_temp.csv")
#    if not filename.is_file():
#        print('Uncompressing filename from tarball ...')
#        filename = "df_temp.tar.bz2"
#        subprocess.Popen(['tar', '-xjvf', filename])  # = tar -xjvf df_temp.tar.bz2
#        time.sleep(5) # pause 5 seconds to give tar extract time to complete prior to attempting pandas read_csv
#   df_temp = pd.read_csv('df_temp.csv', index_col=0)

else:    
    
    print('read stat4.GloSATprelim03.1658-2020.txt ...')

    filename_txt = 'DATA/stat4.GloSATprelim03.1658-2020.txt'
    df = load_dataframe(filename_txt)

    # ADD LEADING 0: TO STATION CODES (str)

    df['stationcode'] = [ str(df['stationcode'][i]).zfill(6) for i in range(len(df)) ]

    # APPLY: SCALE FACTORS
    
    print('apply scale factors ...')

    df['stationlat'] = df['stationlat']/10.0
    df['stationlon'] = df['stationlon']/10.0

    for j in range(1,13):

        df[df.columns[j]] = df[df.columns[j]]/10.0

    # CONVERT: LONGITUDE FROM +W TO +E

    print('convert longitudes ...')

    df['stationlon'] = -df['stationlon']     
        
    # CONVERT: DTYPES FOR EFFICIENT STORAGE

    df['year'] = df['year'].astype('int16')

    for j in range(1,13):

        df[df.columns[j]] = df[df.columns[j]].astype('float32')

    df['stationlat'] = df['stationlat'].astype('float32')
    df['stationlon'] = df['stationlon'].astype('float32')
    df['stationelevation'] = df['stationelevation'].astype('int16')
    df['stationfirstyear'] = df['stationfirstyear'].astype('int16')
    df['stationlastyear'] = df['stationlastyear'].astype('int16')    
    df['stationsource'] = df['stationsource'].astype('int8')    
    df['stationfirstreliable'] = df['stationfirstreliable'].astype('int16')

    # SAVE: TEMPERATURES

    print('save temperatures ...')

    df_temp = df.copy()
    df_temp.to_pickle('df_temp.pkl', compression='bz2')

#------------------------------------------------------------------------------
# LOAD: Normals, SDs and FRYs
#------------------------------------------------------------------------------

if load_df_normals == True:

    print('loading normals ...')

    df_normals = pd.read_pickle('DATA/df_normals.pkl', compression='bz2')    

else:

    print('extracting normals ...')

    file = 'DATA/normals5.GloSAT.prelim03_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'
#    file = 'normals5.GloSAT.prelim02_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'
#    file = 'normals5.GloSAT.prelim01_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'

#    file = 'sd5.GloSAT.prelim03_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'
#    file = 'sd5.GloSAT.prelim02_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'
#    file = 'sd5.GloSAT.prelim01_FRYuse_ocPLAUS1_iqr3.600reg0.3_19411990_MIN15_OCany_19611990_MIN15_PERDEC00_NManySDreq.txt'

#    1 ID
#    2-3 First year, last year
#    4-5 First year for normal, last year for normal
#    6-17 Twelve normals for Jan to Dec, degC (not degC*10), missing normals are -999.000
#    18 Source code (1=missing, 2=Phil's estimate, 3=WMO, 4=calculated here, 5=calculated previously)
#    19-30 Twelve values giving the % data available for computing each normal from the 1961-1990 period

    f = open(file)
    lines = f.readlines()

    stationcodes = []
    firstyears = []
    lastyears = []
    normalfirstyears = []
    normallastyears = []
    sourcecodes = []
    normal12s = []
    normalpercentage12s = []
    
    for i in range(len(lines)):
        words = lines[i].split()
        if len(words) > 1:
            stationcode = words[0].zfill(6)
            firstyear = int(words[1])
            lastyear = int(words[2])
            normalfirstyear = int(words[3])
            normallastyear = int(words[4])
            sourcecode = int(words[17])
            normal12 = words[5:17]
            normalpercentage12 = words[18:31]

            stationcodes.append(stationcode)
            firstyears.append(firstyear)
            lastyears.append(lastyear)
            normalfirstyears.append(normalfirstyear)
            normallastyears.append(normallastyear)
            sourcecodes.append(sourcecode)
            normal12s.append(normal12)
            normalpercentage12s.append(normalpercentage12)
    f.close()
    
    normal12s = np.array(normal12s)
    normalpercentage12s = np.array(normalpercentage12s)

    df_normals = pd.DataFrame({
        'stationcode':stationcodes,
        'firstyear':firstyears, 
        'lastyear':lastyears, 
        'normalfirstyear':normalfirstyears, 
        'normallastyear':normallastyears, 
        'sourcecode':sourcecodes,
        '1':normal12s[:,0],                               
        '2':normal12s[:,1],                               
        '3':normal12s[:,2],                               
        '4':normal12s[:,3],                               
        '5':normal12s[:,4],                               
        '6':normal12s[:,5],                               
        '7':normal12s[:,6],                               
        '8':normal12s[:,7],                               
        '9':normal12s[:,8],                               
        '10':normal12s[:,9],                               
        '11':normal12s[:,10],                               
        '12':normal12s[:,11],                               
        '1pc':normalpercentage12s[:,0],                               
        '2pc':normalpercentage12s[:,1],                               
        '3pc':normalpercentage12s[:,2],                               
        '4pc':normalpercentage12s[:,3],                               
        '5pc':normalpercentage12s[:,4],                               
        '6pc':normalpercentage12s[:,5],                               
        '7pc':normalpercentage12s[:,6],                               
        '8pc':normalpercentage12s[:,7],                               
        '9pc':normalpercentage12s[:,8],                               
        '10pc':normalpercentage12s[:,9],                               
        '11pc':normalpercentage12s[:,10],                               
        '12pc':normalpercentage12s[:,11],                                                              
    })   

    # FILTER OUT: stations with missing normals

#    df_normals_copy = df_normals.copy()
#    df_normals = df_normals_copy[df_normals_copy['sourcecode']>1]

    print('save normals ...')

    df_normals.to_pickle('df_normals.pkl', compression='bz2')
        
#------------------------------------------------------------------------------
# LOAD: anomalies (1961-1990 baseline)
#------------------------------------------------------------------------------

if load_df_anom == True:

    print('loading anomalies ...')

    df_anom = pd.read_pickle('DATA/df_anom.pkl', compression='bz2')    

else:
    
    print('calculate baselines and anomalies ...')

    df_anom = df_temp.copy()
    for i in range(len(df_anom['stationcode'].unique())):
        da = df_anom[df_anom['stationcode']==df_anom['stationcode'].unique()[i]]
        for j in range(1,13):
            baseline = np.nanmean(np.array(da[(da['year']>=1961) & (da['year']<=1990)].iloc[:,j]).ravel())
            df_anom.loc[da.index.tolist(), str(j)] = da[str(j)]-baseline            

    print('save anomalies ...')

    df_anom.to_pickle('df_anom.pkl', compression='bz2')

#------------------------------------------------------------------------------
# LOAD: normalised anomalies
#------------------------------------------------------------------------------
    
if load_df_norm == True:

    print('loading normalized anomalies ...')

    df_norm = pd.read_pickle('DATA/df_norm.pkl', compression='bz2')    
#   df_norm['stationcode'] = df_norm['stationcode'].str.zfill(6)
    
else:

    print('normalize anomalies ...')

    # ADJUST: anomaly timeseries using normalisation for each month
        
    df_norm = df_anom.copy()              
    for i in range(len(df_norm['stationcode'].unique())):            
        da = df_norm[df_norm['stationcode']==df_norm['stationcode'].unique()[i]]
        for j in range(1,13):
            df_norm.loc[da.index.tolist(), str(j)] = (da[str(j)]-da[str(j)].dropna().mean())/da[str(j)].dropna().std()

    print('save normalized anomalies ...')

    df_norm.to_pickle('df_norm.pkl', compression='bz2')
    
#------------------------------------------------------------------------------
# LOAD: Copernicus stations
#------------------------------------------------------------------------------

nheader = 0
f = open('DATA/Copernicus_T_pre1851', encoding="utf8", errors='ignore')
lines = f.readlines()
lats = []
lons = []
firstyears = []
lastyears = [] 
for i in range(nheader,len(lines)):
    words = lines[i]   
    lat = float(words[31:37].strip())
    lon = float(words[37:44].strip())
    first = int(words[44:49].strip())
    last = int(words[49:55].strip())
    lats.append(lat)
    lons.append(lon)
    firstyears.append(first)
    lastyears.append(last)
f.close()    

df_copernicus = pd.DataFrame({'stationlat':lats, 'stationlon':lons, 'stationfirstyear':firstyears, 'stationlastyear':lastyears})

#------------------------------------------------------------------------------
# EXTRACT: difference list
#------------------------------------------------------------------------------

print('extracting_difference_list ...')

decimalplaces = 1  
    
df_anom_in = df_anom.copy()
df_normals = pd.read_pickle('DATA/df_normals.pkl', compression='bz2')
ds_all = df_anom_in[df_anom_in['stationcode'].isin(df_normals[df_normals['sourcecode']>1]['stationcode'])]
ds_glosat = ds_all[(ds_all['stationfirstyear']<=end_year)&(ds_all['stationlastyear']>=start_year)]
ds_glosat_lon = ds_glosat.dropna().groupby('stationcode').mean()['stationlon'].apply(lambda x: round(x, decimalplaces))
ds_glosat_lat = ds_glosat.dropna().groupby('stationcode').mean()['stationlat'].apply(lambda x: round(x, decimalplaces))
ds_glosat_firstyear = ds_glosat.dropna().groupby('stationcode').mean()['stationfirstyear'].astype('int')
ds_glosat_lastyear = ds_glosat.dropna().groupby('stationcode').mean()['stationlastyear'].astype('int')
ds_glosat_stationcode = ds_glosat['stationcode'].unique().astype(int)
ds_glosat_lonlat = pd.DataFrame({'stationlon':ds_glosat_lon,'stationlat':ds_glosat_lat,'stationfirstyear':ds_glosat_firstyear,'stationlastyear':ds_glosat_lastyear,'stationcode':ds_glosat_stationcode}).reset_index(drop=True)

ds = df_copernicus.copy()
ds_copernicus = ds[(ds['stationfirstyear']<=end_year) & (ds['stationlastyear']>=start_year)]
ds_copernicus_lon = ds_copernicus['stationlon'].apply(lambda x: round(x, decimalplaces))
ds_copernicus_lat = ds_copernicus['stationlat'].apply(lambda x: round(x, decimalplaces))
ds_copernicus_firstyear = ds_copernicus['stationfirstyear']
ds_copernicus_lastyear = ds_copernicus['stationlastyear']
ds_copernicus_stationcode = len(ds_copernicus['stationlastyear'])*np.nan
ds_copernicus_lonlat = pd.DataFrame({'stationlon':ds_copernicus_lon,'stationlat':ds_copernicus_lat,'stationfirstyear':ds_copernicus_firstyear,'stationlastyear':ds_copernicus_lastyear,'stationcode':ds_copernicus_stationcode}).reset_index(drop=True)

ds_glosat_copernicus_unique = pd.concat([ds_glosat_lonlat,ds_copernicus_lonlat]).drop_duplicates(subset=['stationlon', 'stationlat'], keep=False).reset_index(drop=True)
mask = np.isfinite(ds_glosat_copernicus_unique['stationcode'])
ds_glosat_minus_copernicus = ds_glosat_copernicus_unique[mask].reset_index(drop=True)
ds_copernicus_minus_glosat = ds_glosat_copernicus_unique[~mask].reset_index(drop=True)
stationcodes = [ str(int(ds_glosat_minus_copernicus['stationcode'][i])).zfill(6) for i in range(len(ds_glosat_minus_copernicus))]
ds_glosat_minus_copernicus['stationcode'] = stationcodes

ds_glosat_minus_copernicus.to_csv(output_dir + '/' + 'ds_glosat_minus_copernicus.csv')
ds_copernicus_minus_glosat.to_csv(output_dir + '/' + 'ds_copernicus_minus_glosat.csv')

#------------------------------------------------------------------------------
# PLOT: stations on world map
#------------------------------------------------------------------------------

if plot_station_months_map_copernicus == True:

    print('plot_station_months_map_copernicus ...')
    
    ds = df_copernicus.copy()
    dg = ds[(ds['stationfirstyear']<=end_year) & (ds['stationlastyear']>=start_year)]
    station_months = (dg['stationlastyear']-dg['stationfirstyear']+1)*12
#    dg['stationmonths'] = station_months
#    v = dg['stationmonths']
    v = station_months
    x, y = np.meshgrid(dg['stationlon'], dg['stationlat'])            
    
    figstr = 'location_map_copernicus.png'
    titlestr = 'C3S stations with observations spanning ('+str(start_year)+'-'+str(end_year)+') \n' + 'N(stations)='+str(len(dg)) + ', N(months)='+str(np.sum(station_months)) + ', median N(years)='+str(np.median(np.floor(station_months/12)))
     
#   colorbarstr = 'Station months'
    colorbarstr = ''
    cmap = 'magma'

    fig  = plt.figure(figsize=(15,10))
    projection = 'robinson'    
    if projection == 'platecarree': p = ccrs.PlateCarree(central_longitude=0); threshold = 0
    if projection == 'mollweide': p = ccrs.Mollweide(central_longitude=0); threshold = 1e6
    if projection == 'robinson': p = ccrs.Robinson(central_longitude=0); threshold = 0
    if projection == 'equalearth': p = ccrs.EqualEarth(central_longitude=0); threshold = 0
    if projection == 'geostationary': p = ccrs.Geostationary(central_longitude=0); threshold = 0
    if projection == 'goodehomolosine': p = ccrs.InterruptedGoodeHomolosine(central_longitude=0); threshold = 0
    if projection == 'europp': p = ccrs.EuroPP(); threshold = 0
    if projection == 'northpolarstereo': p = ccrs.NorthPolarStereo(); threshold = 0
    if projection == 'southpolarstereo': p = ccrs.SouthPolarStereo(); threshold = 0
    if projection == 'lambertconformal': p = ccrs.LambertConformal(central_longitude=0); threshold = 0
    if projection == 'winkeltripel': p = ccrs.WinkelTripel(central_longitude=0); threshold = 0
    ax = plt.axes(projection=p)
    ax.set_global()
#   ax.stock_img()
#   ax.add_feature(cf.COASTLINE, edgecolor="lightblue")
#   ax.add_feature(cf.BORDERS, edgecolor="lightblue")
#   ax.coastlines(color='lightblue')
    ax.coastlines()
    ax.gridlines()    
        
    g = ccrs.Geodetic()
    trans = ax.projection.transform_points(g, x, y)
    x0 = trans[:,:,0]
    x1 = trans[:,:,1]

    if projection == 'platecarree':
        ax.set_extent([-180, 180, -90, 90], crs=p)    
        gl = ax.gridlines(crs=p, draw_labels=True, linewidth=1, color='black', alpha=0.5, linestyle='-')
        gl.xlabels_top = False
        gl.ylabels_right = False
        gl.xlines = True
        gl.ylines = True
        gl.xlocator = mticker.FixedLocator([-180,-120,-60,0,60,120,180])
        gl.ylocator = mticker.FixedLocator([-90,-60,-30,0,30,60,90])
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {'size': fontsize}
        gl.ylabel_style = {'size': fontsize}              
    ax.add_feature(cartopy.feature.OCEAN, zorder=100, alpha=0.2, edgecolor='k')
    plt.scatter(x=dg['stationlon'], y=dg['stationlat'], 
                c=np.log10(station_months), s=50, alpha=1.0, 
                transform=ccrs.PlateCarree(), cmap=cmap)     
#    plt.scatter(x=df_copernicus['stationlon'], y=df_copernicus['stationlat'], 
#                c=df_copernicus['stationmonths'], s=50, alpha=1.0, 
#                transform=ccrs.PlateCarree(), cmap=cmap)     
    cb = plt.colorbar(orientation="horizontal", shrink=0.7, pad=0.05, extend='both')     
    cb.set_label(colorbarstr, labelpad=0, fontsize=fontsize)
    cb.ax.set_xticks(['1','','10','','100','','1000','','10000']) 
    cb.ax.set_xticklabels(['1','','10','','100','','1000','','10000']) 
    cb.ax.tick_params(labelsize=fontsize)
    plt.title(titlestr, fontsize=fontsize, pad=20)
    plt.savefig(figstr)
    plt.close('all')
    
#------------------------------------------------------------------------------
# PLOT: stations on world map coloured by number of months
#------------------------------------------------------------------------------

if plot_station_months_map_glosat == True:

    print('plot_station_months_map_glosat ...')
    
    df_anom_in = df_anom.copy()
    df_normals = pd.read_pickle('DATA/df_normals.pkl', compression='bz2')
    ds_all = df_anom_in[df_anom_in['stationcode'].isin(df_normals[df_normals['sourcecode']>1]['stationcode'])]
    ds = ds_all[(ds_all['stationfirstyear']<=end_year)&(ds_all['stationlastyear']>=start_year)]
    lon = ds.groupby('stationcode').mean()['stationlon']
    lat = ds.groupby('stationcode').mean()['stationlat']

    stationmonths=[]
    for i in range(12):
        months = np.array(ds.groupby(['stationcode'])[str(i+1)].count())
        stationmonths.append(months)
    stationmonths = np.nansum(stationmonths,axis=0)

#   stationmonths = (ds['stationlastyear']-ds['stationfirstyear']+1)*12
    dg = pd.DataFrame({'lon':lon, 'lat':lat, 'stationmonths':stationmonths})
#    dg = df[df['stationmonths']>0] 
#   v = np.log10(stationmonths)
    v = dg['stationmonths']
    x, y = np.meshgrid(dg['lon'], dg['lat'])    
        
#    vmin = np.min(v)
#    vmax = np.max(v)
#   vmin = np.percentile(v,25)
#   vmax = np.percentile(v,75)
    
    figstr = 'location_map_glosat.png'
    titlestr = 'GloSAT stations with observations spanning ('+str(start_year)+'-'+str(end_year)+') \n' + 'N(stations)='+str(len(dg)) + ', N(months)='+str(np.sum(dg['stationmonths'])) + ', median N(years)='+str(np.median(np.floor(dg['stationmonths']/12)))

#   colorbarstr = 'Station months'
    colorbarstr = ''
    cmap = 'magma'

    fig  = plt.figure(figsize=(15,10))
    projection = 'robinson'    
    if projection == 'platecarree': p = ccrs.PlateCarree(central_longitude=0); threshold = 0
    if projection == 'mollweide': p = ccrs.Mollweide(central_longitude=0); threshold = 1e6
    if projection == 'robinson': p = ccrs.Robinson(central_longitude=0); threshold = 0
    if projection == 'equalearth': p = ccrs.EqualEarth(central_longitude=0); threshold = 0
    if projection == 'geostationary': p = ccrs.Geostationary(central_longitude=0); threshold = 0
    if projection == 'goodehomolosine': p = ccrs.InterruptedGoodeHomolosine(central_longitude=0); threshold = 0
    if projection == 'europp': p = ccrs.EuroPP(); threshold = 0
    if projection == 'northpolarstereo': p = ccrs.NorthPolarStereo(); threshold = 0
    if projection == 'southpolarstereo': p = ccrs.SouthPolarStereo(); threshold = 0
    if projection == 'lambertconformal': p = ccrs.LambertConformal(central_longitude=0); threshold = 0
    if projection == 'winkeltripel': p = ccrs.WinkelTripel(central_longitude=0); threshold = 0
    ax = plt.axes(projection=p)
    ax.set_global()
#   ax.stock_img()
#   ax.add_feature(cf.COASTLINE, edgecolor="lightblue")
#   ax.add_feature(cf.BORDERS, edgecolor="lightblue")
#   ax.coastlines(color='lightblue')
    ax.coastlines()
    ax.gridlines()    
        
    g = ccrs.Geodetic()
    trans = ax.projection.transform_points(g, x, y)
    x0 = trans[:,:,0]
    x1 = trans[:,:,1]

    if projection == 'platecarree':
        ax.set_extent([-180, 180, -90, 90], crs=p)    
        gl = ax.gridlines(crs=p, draw_labels=True, linewidth=1, color='black', alpha=0.5, linestyle='-')
        gl.xlabels_top = False
        gl.ylabels_right = False
        gl.xlines = True
        gl.ylines = True
        gl.xlocator = mticker.FixedLocator([-180,-120,-60,0,60,120,180])
        gl.ylocator = mticker.FixedLocator([-90,-60,-30,0,30,60,90])
        gl.xformatter = LONGITUDE_FORMATTER
        gl.yformatter = LATITUDE_FORMATTER
        gl.xlabel_style = {'size': fontsize}
        gl.ylabel_style = {'size': fontsize}              
    ax.add_feature(cartopy.feature.OCEAN, zorder=100, alpha=0.2, edgecolor='k')
    plt.scatter(x=dg['lon'], y=dg['lat'], 
                c=np.log10(dg['stationmonths']), s=50, alpha=1.0,
                transform=ccrs.PlateCarree(), cmap=cmap)  
#    plt.scatter(x=dg['lon'], y=dg['lat'], 
#                c=dg['stationmonths'], s=50, alpha=1.0,
#                transform=ccrs.PlateCarree(), cmap=cmap)  
    cb = plt.colorbar(orientation="horizontal", shrink=0.7, pad=0.05, extend='both')    
    cb.set_label(colorbarstr, labelpad=0, fontsize=fontsize)
    cb.ax.set_xticks(['1','','10','','100','','1000','','10000']) 
    cb.ax.set_xticklabels(['1','','10','','100','','1000','','10000']) 
    cb.ax.tick_params(labelsize=fontsize)
    plt.title(titlestr, fontsize=fontsize, pad=20)
    plt.savefig(figstr)
    plt.close('all')
        
#------------------------------------------------------------------------------
print('** END')

