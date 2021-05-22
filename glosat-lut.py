#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: glosat-lut.py
#-----------------------------------------------------------------------
# Version 0.1
# 22 May, 2021
# Dr Michael Taylor
# https://patternizer.github.io
# patternizer AT gmail DOT com
# michael DOT a DOT taylor AT uea DOT ac DOT uk
#-----------------------------------------------------------------------

# Dataframe libraries:
import numpy as np
import pandas as pd
import pickle

# I/O libraries:
import os, glob

#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

fontsize = 16
data_dir = 'DATA/'
output_dir = 'OUT/'

#-----------------------------------------------------------------------------
# LOAD: inventories into dataframes
#-----------------------------------------------------------------------------

print('loading inventories ...')

df_lut1 = pd.read_excel(open(data_dir+'Olg_Belgian_African_stns_inventoried.xlsx', 'rb'), sheet_name='c3s') 
df_lut2 = pd.read_excel(open(data_dir+'processed_monthly_temp_africa.xlsx', 'rb')) 
df_lut3 = pd.read_excel(open(data_dir+'africa_temp_monthly_raw.xlsx', 'rb')) 
df_lut4 = pd.read_pickle(data_dir+'df_temp.pkl', compression='bz2')

#-----------------------------------------------------------------------------
# CONSTRUCT: look-up tables (LUT): cru_code, lat, lon, elevation, name, country, firstyear, lastyear, source_id
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
# LUT1 dataset: Courtesy of Simon Noone, Peter Thorne and Robert Dunn
#-----------------------------------------------------------------------------
#Index(['station_id', 'source_uid', 'source_name', 'station name', 'lat', 'lon',
#       'elev', 'country', 'observedVariable-measurand_variable', 'temporal',
#       'temporalExtent'],
#      dtype='object')
#-----------------------------------------------------------------------------

print('constructing LUT #1 ...')


stationcodes = []
firstyears = []
lastyears = []
elevations = []
for i in range(len(df_lut1)):
    if str(df_lut1['station_id'][i])[-5:] == '00000':
        stationcodes.append(str(df_lut1['station_id'][i])[:-5])
    else:                            
        stationcodes.append(-999)
    if str(df_lut1['temporalExtent'][i]) == 'nan':
        firstyears.append(-999)
        lastyears.append(-999)
    else:    
        firstyears.append(int(str(df_lut1['temporalExtent'][i])[0:4]))
        lastyears.append(int(str(df_lut1['temporalExtent'][i])[5:10]))
    if str(df_lut1['elev'][i]) == 'nan':
        elevations.append(-999)
    else:
        elevations.append(int(df_lut1['elev'][i]))
df_lut1['cru_code'] = stationcodes
df_lut1['firstyear'] = firstyears
df_lut1['lastyear'] = lastyears
df_lut1['elev'] = elevations

# CONVERT: to CRUTEM format (lat*10, lon*10, missing=-999)

lut1 = pd.DataFrame(columns=[
    'station_code',
    'station_lat',
    'station_lon',
    'station_elevation',
    'station_name',
    'station_country',
    'station_firstyear',
    'station_lastyear',
    'source_code1',
    'source_code2'
    ])
lut1['station_code'] = df_lut1['cru_code']
lut1['station_lat'] = [ int(np.round(df_lut1['lat'][i]*10)) for i in range(len(lut1)) ]
lut1['station_lon'] = [ int(np.round(df_lut1['lon'][i]*10)) for i in range(len(lut1)) ]
lut1['station_elevation'] = df_lut1['elev']
lut1['station_name'] = df_lut1['station name']
lut1['station_country'] = df_lut1['country']
lut1['station_firstyear'] = df_lut1['firstyear']
lut1['station_lastyear'] = df_lut1['lastyear']
lut1['source_code1'] = df_lut1['station_id']
lut1['source_code2'] = len(df_lut1['station_id'])*[str(-999)]
lut1 = lut1.astype({'station_code': 'string', 'station_name': 'string', 'station_country': 'string', 'source_code1': 'string', 'source_code2': 'string'})

# lut1[lut1.source_code==60677100000]['station_name']

#-----------------------------------------------------------------------------
# LUT2 dataset: Courtesy of Simon Noone, Peter Thorne and Robert Dunn
#-----------------------------------------------------------------------------
#Index(['promary_id', 'secondary_id', 'station_na', 'longitude', 'latitude',
#       'height_of_', 'start_date', 'end_date', 'region', 'source_id',
#       'Country _id', 'Country', 'ISO'],
#      dtype='object')
#-----------------------------------------------------------------------------

print('constructing LUT #2 ...')

stationcodes = []
elevations = []
for i in range(len(df_lut2)):
    if str(df_lut2['secondary_id'][i])[-5:] == '99999':
        stationcodes.append(str(df_lut2['secondary_id'][i])[:-5])
    elif len(str(df_lut2['secondary_id'][i])) == 9:
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')
    elif len(str(df_lut2['secondary_id'][i])) == 7:
        stationcodes.append(str(df_lut2['secondary_id'][i])[0:5]+'0')
    elif len(str(df_lut2['secondary_id'][i])) == 5:
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')
    elif str(df_lut2['secondary_id'][i])[0:3] == 'WMO':
        stationcodes.append(str(df_lut2['secondary_id'][i])[3:8]+'0')        
    elif str(df_lut2['secondary_id'][i])[0:4] == 'AFWA':
        stationcodes.append(str(df_lut2['secondary_id'][i])[4:10])      
    elif str(df_lut2['secondary_id'][i])[0:5] == '000RR':
        stationcodes.append(str(df_lut2['secondary_id'][i])[-6:])                      
    elif str(df_lut2['secondary_id'][i])[0:5] == 'EGY00':
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1030000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1050000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1090000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1100000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1120000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1130000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1180000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1280000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1310000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1330000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1370000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == '1510000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:7] == 'MXN0000':
        stationcodes.append('6'+str(df_lut2['secondary_id'][i])[-4:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:6] == '117000':
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:6] == '141000':
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')                      
    elif str(df_lut2['secondary_id'][i])[0:6] == '651000':
        stationcodes.append(str(df_lut2['secondary_id'][i])[-5:]+'0')                      
    else:                            
        stationcodes.append(-999)        
    if str(df_lut2['height_of_'][i]) == 'nan':
        elevations.append(-999)
    else:
        elevations.append(int(df_lut2['height_of_'][i]))
df_lut2['cru_code'] = stationcodes
df_lut2['height_of_'] = elevations

# CONVERT: to CRUTEM format (lat*10, lon*10, missing=-999)

lut2 = pd.DataFrame(columns=[
    'station_code',
    'station_lat',
    'station_lon',
    'station_elevation',
    'station_name',
    'station_country',
    'station_firstyear',
    'station_lastyear',
    'source_code1',
    'source_code2'
    ])
lut2['station_code'] = df_lut2['cru_code']
lut2['station_lat'] = [ int(np.round(df_lut2['latitude'][i]*10)) for i in range(len(lut2)) ]
lut2['station_lon'] = [ int(np.round(df_lut2['longitude'][i]*10)) for i in range(len(lut2)) ]
lut2['station_elevation'] = df_lut2['height_of_']
lut2['station_name'] = df_lut2['station_na']
lut2['station_country'] = df_lut2['Country']
lut2['station_firstyear'] = df_lut2['start_date']
lut2['station_lastyear'] = df_lut2['end_date']
lut2['source_code1'] = df_lut2['promary_id']
lut2['source_code2'] = df_lut2['secondary_id']
lut2 = lut2.astype({'station_code': 'string', 'station_name': 'string', 'station_country': 'string', 'source_code1': 'string', 'source_code2': 'string'})

#-----------------------------------------------------------------------------
# LUT3 dataset: Courtesy of Simon Noone, Peter Thorne and Robert Dunn
#-----------------------------------------------------------------------------
#Index(['station_id', 'source_uid_1', 'source_name', 'descriptionDataset',
#       'data_repository_ftp', 'station_name', 'lat', 'lon', 'elev',
#       'fips_code', 'country', 'continent', 'station_data_policy',
#       'Temp_start_year', 'Temp_end_year', 'WMO_RA', 'Region'],
#      dtype='object')
#-----------------------------------------------------------------------------

print('constructing LUT #3 ...')

stationcodes = []
elevations = []
for i in range(len(df_lut3)):

    if len(str(df_lut3['station_id'][i])) == 6:
        stationcodes.append(str(df_lut3['station_id'][i]))
    elif len(str(df_lut3['station_id'][i])) == 5:
        stationcodes.append(str(df_lut3['station_id'][i])+'0')
    else:                            
        stationcodes.append(-999)
    if str(df_lut3['elev'][i]) == 'nan':
        elevations.append(-999)
    else:
        elevations.append(int(df_lut3['elev'][i]))
df_lut3['cru_code'] = stationcodes
df_lut3['elev'] = elevations

# CONVERT: to CRUTEM format (lat*10, lon*10, missing=-999)

lut3 = pd.DataFrame(columns=[
    'station_code',
    'station_lat',
    'station_lon',
    'station_elevation',
    'station_name',
    'station_country',
    'station_firstyear',
    'station_lastyear',
    'source_code1',
    'source_code2'
    ])
lut3['station_code'] = df_lut3['cru_code']
lut3['station_lat'] = [ int(np.round(df_lut3['lat'][i]*10)) for i in range(len(lut3)) ]
lut3['station_lon'] = [ int(np.round(df_lut3['lon'][i]*10)) for i in range(len(lut3)) ]
lut3['station_elevation'] = df_lut3['elev']
lut3['station_name'] = df_lut3['station_name']
lut3['station_country'] = df_lut3['country']
lut3['station_firstyear'] = df_lut3['Temp_start_year']
lut3['station_lastyear'] = df_lut3['Temp_end_year']
lut3['source_code1'] = df_lut3['station_id']
lut3['source_code2'] = len(df_lut3['station_id'])*[str(-999)]
lut3 = lut3.astype({'station_code': 'string', 'station_name': 'string', 'station_country': 'string', 'source_code1': 'string', 'source_code2': 'string'})

#-----------------------------------------------------------------------------
# LUT4 dataset: GloSAT.p03
#-----------------------------------------------------------------------------
#Index(['year', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
#       'stationcode', 'stationlat', 'stationlon', 'stationelevation',
#       'stationname', 'stationcountry', 'stationfirstyear', 'stationlastyear',
#       'stationsource', 'stationfirstreliable'],
#      dtype='object')
#-----------------------------------------------------------------------------
    
print('constructing LUT #4 ...')

lut4 = pd.DataFrame(columns=[
    'station_code',
    'station_lat',
    'station_lon',
    'station_elevation',
    'station_name',
    'station_country',
    'station_firstyear',
    'station_lastyear',
    'source_code1',
    'source_code2'
    ])

stationcodes = df_lut4['stationcode'].unique()
lats = df_lut4.groupby('stationcode').mean()['stationlat'].values
lons = df_lut4.groupby('stationcode').mean()['stationlon'].values
elevations = df_lut4.groupby('stationcode').mean()['stationelevation'].values
stationlats = pd.Series(lats*10.0).replace(np.nan,-999)
stationlons = pd.Series(lons*10.0).replace(np.nan,-999)
stationelevations = pd.Series(elevations).replace(np.nan,-999)
nameslist = df_lut4.groupby(['stationcode','stationname']).mean().index
stationnames = [ nameslist[i][1] for i in range(len(nameslist)) ]
countrieslist = df_lut4.groupby(['stationcode','stationcountry']).mean().index
stationcountries = [ countrieslist[i][1] for i in range(len(countrieslist)) ]
stationfirstyears = df_lut4.groupby('stationcode')['stationfirstyear'].mean()
stationlastyears = df_lut4.groupby('stationcode')['stationlastyear'].mean()    
    
#lut4['station_code'] = [ str(stationcodes[i]).zfill(6) for i in range(len(stationcodes)) ]
lut4['station_code'] = [ str(stationcodes[i]) for i in range(len(stationcodes)) ]
lut4['station_lat'] = [ int(np.round(stationlats[i])) for i in range(len(stationcodes)) ]
lut4['station_lon'] = [ int(np.round(stationlons[i])) for i in range(len(stationcodes)) ]
lut4['station_elevation'] = [ int(np.round(stationelevations[i]*1)) for i in range(len(stationcodes)) ]
lut4['station_name'] = stationnames
lut4['station_country'] = stationcountries
lut4['station_firstyear'] = [ int(stationfirstyears[i]) for i in range(len(stationcodes)) ] 
lut4['station_lastyear'] = [ int(stationlastyears[i]) for i in range(len(stationcodes)) ] 
lut4['source_code1'] = len(df_lut4['stationcode'].unique())*[str(-999)]
lut4['source_code2'] = len(df_lut4['stationcode'].unique())*[str(-999)]
lut4 = lut4.astype({'station_code': 'string', 'station_name': 'string', 'station_country': 'string', 'source_code1': 'string', 'source_code2': 'string'})

#-----------------------------------------------------------------------------
# MERGE: LUTs
#-----------------------------------------------------------------------------

print('merging LUTs ...')

lut = pd.concat([lut1,lut2,lut3,lut4], axis=0).reset_index(drop=True)
lut['station_name'] = [ str(lut['station_name'][i]).title() for i in range(len(lut)) ]
lut['station_country'] = [ str(lut['station_country'][i]).upper() for i in range(len(lut)) ]
lut = lut.astype({'station_code': 'string', 'station_name': 'string', 'station_country': 'string', 'source_code1': 'string', 'source_code2': 'string'})

#-----------------------------------------------------------------------------
# SORT: LUTs
#-----------------------------------------------------------------------------

print('sorting LUTs ...')

lut['Nyr'] = lut['station_lastyear'] - lut['station_firstyear'] + 1
lut_name = lut.sort_values(by='station_name').reset_index(drop=True)
lut_Nyr = lut.sort_values(by='Nyr', ascending=False).reset_index(drop=True)
lut_firstyear = lut.sort_values(by='station_firstyear', ascending=True).reset_index(drop=True)

#-----------------------------------------------------------------------------
# WRITE: LUTs to CSV
#-----------------------------------------------------------------------------

print('writing LUTs to CSV ...')

lut1.to_csv( output_dir + 'lut1.csv')
lut2.to_csv( output_dir + 'lut2.csv')
lut3.to_csv( output_dir + 'lut3.csv')
lut4.to_csv( output_dir + 'lut4.csv')
lut.to_csv( output_dir + 'lut.csv')
lut_name.to_csv( output_dir + 'lut_sortedby_name.csv')
lut_Nyr.to_csv( output_dir + 'lut_sortedby_Nyr.csv')
lut_firstyear.to_csv( output_dir + 'lut_sortedby_firstyear.csv')

#-----------------------------------------------------------------------------
# LUT: stats
#-----------------------------------------------------------------------------

print('calculating LUT stats ...')

N_remaining_lut1 = len(lut1[lut1['station_code']=='-999'])
N_remaining_lut2 = len(lut2[lut2['station_code']=='-999'])
N_remaining_lut3 = len(lut3[lut3['station_code']=='-999'])
N_remaining_lut4 = len(lut4[lut4['station_code']=='-999'])
N_remaining_lut = len(lut[lut['station_code']=='-999'])
print('LUT1: N(not ID)=',str(N_remaining_lut1),'out of N=',str(len(lut1)))
print('LUT2: N(not ID)=',str(N_remaining_lut2),'out of N=',str(len(lut2)))
print('LUT3: N(not ID)=',str(N_remaining_lut3),'out of N=',str(len(lut3)))
print('LUT4: N(not ID)=',str(N_remaining_lut4),'out of N=',str(len(lut4)))
print('LUT: N(not ID)=',str(N_remaining_lut),'out of N=',str(len(lut)))

#-----------------------------------------------------------------------------
print('** END')