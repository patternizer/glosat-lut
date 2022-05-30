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

#import pyproj
#import shapely
#import fiona

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

glosat_version = 'GloSAT.p04'

#------------------------------------------------------------------------------
# LOAD: absolute temperatures
#------------------------------------------------------------------------------

print('loading temperatures ...')

df_temp = pd.read_pickle('DATA/df_temp.pkl', compression='bz2')    
    
#------------------------------------------------------------------------------
# LOAD: Normals, SDs and FRYs
#------------------------------------------------------------------------------

print('loading normals ...')

df_normals = pd.read_pickle('DATA/df_normals.pkl', compression='bz2')    
            
#------------------------------------------------------------------------------
# LOAD: anomalies (1961-1990 baseline)
#------------------------------------------------------------------------------

print('loading anomalies ...')

df_anom = pd.read_pickle('DATA/df_anom.pkl', compression='bz2')    

#------------------------------------------------------------------------------
# LOAD: Copernicus stations
#------------------------------------------------------------------------------

print('loading C3S stations ...')

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
ds_glosat_stationcode = ds_glosat_lon.index
ds_glosat_lonlat = pd.DataFrame({'stationlon':ds_glosat_lon,'stationlat':ds_glosat_lat,'stationfirstyear':ds_glosat_firstyear,'stationlastyear':ds_glosat_lastyear,'stationcode':ds_glosat_stationcode}).reset_index(drop=True)

ds = df_copernicus.copy()
ds_copernicus = ds[(ds['stationfirstyear']<=end_year) & (ds['stationlastyear']>=start_year)]
ds_copernicus_lon = ds_copernicus['stationlon'].apply(lambda x: round(x, decimalplaces))
ds_copernicus_lat = ds_copernicus['stationlat'].apply(lambda x: round(x, decimalplaces))
ds_copernicus_firstyear = ds_copernicus['stationfirstyear']
ds_copernicus_lastyear = ds_copernicus['stationlastyear']
ds_copernicus_stationcode = len(ds_copernicus['stationlastyear'])*'-999'
ds_copernicus_lonlat = pd.DataFrame({'stationlon':ds_copernicus_lon,'stationlat':ds_copernicus_lat,'stationfirstyear':ds_copernicus_firstyear,'stationlastyear':ds_copernicus_lastyear,'stationcode':ds_copernicus_stationcode}).reset_index(drop=True)

ds_glosat_copernicus_unique = pd.concat([ds_glosat_lonlat,ds_copernicus_lonlat]).drop_duplicates(subset=['stationlon', 'stationlat'], keep=False).reset_index(drop=True)
mask = ds_glosat_copernicus_unique['stationcode'] == '-999'
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
    v = station_months
    x, y = np.meshgrid(dg['stationlon'], dg['stationlat'])            
    
    figstr = 'location_map_copernicus.png'
    titlestr = 'C3S stations with observations spanning ('+str(start_year)+'-'+str(end_year)+') \n' + 'N(stations)='+str(len(dg)) + ', N(months)='+str(np.sum(station_months)) + ', median N(years)='+str(np.median(np.floor(station_months/12)))
     
    colorbarstr = 'station months'
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
    ax.stock_img()
#   ax.add_feature(cf.COASTLINE, edgecolor="lightblue")
#   ax.add_feature(cf.BORDERS, edgecolor="lightblue")
#   ax.coastlines(color='lightblue')
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
#    ax.add_feature(cartopy.feature.OCEAN, zorder=100, alpha=0.2, edgecolor='k')
#    plt.scatter(x=dg['stationlon'], y=dg['stationlat'], c=np.log10(station_months), s=50, alpha=1.0, 
#                transform=ccrs.PlateCarree(), cmap=cmap)     
    plt.scatter(x=dg['stationlon'], y=dg['stationlat'], c=station_months, s=50, alpha=1.0, 
                transform=ccrs.PlateCarree(), cmap=cmap)     
    cb = plt.colorbar(orientation="horizontal", shrink=0.7, pad=0.05, extend='both')     
    cb.set_label(colorbarstr, labelpad=0, fontsize=fontsize)
#    cb.ax.set_xticks(['1','','10','','100','','1000','','10000']) 
#    cb.ax.set_xticklabels(['1','','10','','100','','1000','','10000']) 
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

    dg = pd.DataFrame({'lon':lon, 'lat':lat, 'stationmonths':stationmonths})
    v = dg['stationmonths']
    x, y = np.meshgrid(dg['lon'], dg['lat'])    
        
    figstr = 'location_map_glosat.png'
    titlestr = glosat_version + ' stations with observations spanning ('+str(start_year)+'-'+str(end_year)+') \n' + 'N(stations)='+str(len(dg)) + ', N(months)='+str(np.sum(dg['stationmonths'])) + ', median N(years)='+str(np.median(np.floor(dg['stationmonths']/12)))

    colorbarstr = 'Station months'
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
    ax.stock_img()
#   ax.add_feature(cf.COASTLINE, edgecolor="lightblue")
#   ax.add_feature(cf.BORDERS, edgecolor="lightblue")
#   ax.coastlines(color='lightblue')
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
#    ax.add_feature(cartopy.feature.OCEAN, zorder=100, alpha=0.2, edgecolor='k')
#    plt.scatter(x=dg['lon'], y=dg['lat'], c=np.log10(dg['stationmonths']), s=50, alpha=1.0,
#                transform=ccrs.PlateCarree(), cmap=cmap)  
    plt.scatter(x=dg['lon'], y=dg['lat'], c=dg['stationmonths'], s=50, alpha=1.0,
                transform=ccrs.PlateCarree(), cmap=cmap)  
    cb = plt.colorbar(orientation="horizontal", shrink=0.7, pad=0.05, extend='both')    
    cb.set_label(colorbarstr, labelpad=0, fontsize=fontsize)
#    cb.ax.set_xticks(['1','','10','','100','','1000','','10000']) 
#    cb.ax.set_xticklabels(['1','','10','','100','','1000','','10000']) 
    cb.ax.tick_params(labelsize=fontsize)
    plt.title(titlestr, fontsize=fontsize, pad=20)
    plt.savefig(figstr)
    plt.close('all')
        
#------------------------------------------------------------------------------
print('** END')

