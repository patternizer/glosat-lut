#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: glosat-lut.py
#-----------------------------------------------------------------------
# Version 0.3
# 25 May, 2022
# Dr Michael Taylor
# https://patternizer.github.io
# patternizer AT gmail DOT com
# michael DOT a DOT taylor AT uea DOT ac DOT uk
#-----------------------------------------------------------------------

# Dataframe libraries:
import numpy as np
import pandas as pd
import pickle
import re
import difflib

# I/O libraries:
import os, glob

# ISO-3166 libraries:
import pycountry

#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

fontsize = 16
data_dir = 'DATA/'
output_dir = 'OUT/'
coords_dp = 1

#-----------------------------------------------------------------------------
# METHODS:
#-----------------------------------------------------------------------------

def strip_character(dataCol):
    #ascii_alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
#    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()[\]{}\']')
    r = re.compile(r'[^a-zA-Z !@#$%&*_+-=|\:";<>,./()]')
    return r.sub('', dataCol)

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
        firstyears.append(-9999)
        lastyears.append(-9999)
    else:    
        firstyears.append(int(str(df_lut1['temporalExtent'][i])[0:4]))
        lastyears.append(int(str(df_lut1['temporalExtent'][i])[5:10]))
    if str(df_lut1['elev'][i]) == 'nan':
        elevations.append(-9999)
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
    'source_code2',
    'source_lut',
    'iso-3166',
    'continent',
    ])
lut1['station_code'] = df_lut1['cru_code']
lut1['station_lat'] = [ int(np.round(df_lut1['lat'][i]*10**coords_dp)) for i in range(len(lut1)) ]
lut1['station_lon'] = [ int(np.round(df_lut1['lon'][i]*10**coords_dp)) for i in range(len(lut1)) ]
lut1['station_elevation'] = df_lut1['elev']
lut1['station_name'] = df_lut1['station name']
lut1['station_country'] = df_lut1['country']
lut1['station_firstyear'] = df_lut1['firstyear']
lut1['station_lastyear'] = df_lut1['lastyear']
lut1['source_code1'] = df_lut1['station_id']
lut1['source_code2'] = len(lut1)*[str(-999)]
lut1['source_lut'] = len(lut1)*[1]
lut1['iso-3166'] = len(lut1)*[str(-999)]
lut1['continent'] = len(lut1)*[str(-999)]
     
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
        elevations.append(-9999)
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
    'source_code2',
    'source_lut',
    'iso-3166',
    'continent',
    ])
lut2['station_code'] = df_lut2['cru_code']
lut2['station_lat'] = [ int(np.round(df_lut2['latitude'][i]*10**coords_dp)) for i in range(len(lut2)) ]
lut2['station_lon'] = [ int(np.round(df_lut2['longitude'][i]*10**coords_dp)) for i in range(len(lut2)) ]
lut2['station_elevation'] = df_lut2['height_of_']
lut2['station_name'] = df_lut2['station_na']
lut2['station_country'] = df_lut2['Country']
lut2['station_firstyear'] = df_lut2['start_date']
lut2['station_lastyear'] = df_lut2['end_date']
lut2['source_code1'] = df_lut2['promary_id']
lut2['source_code2'] = df_lut2['secondary_id']
lut2['source_lut'] = len(lut2)*[2]
lut2['iso-3166'] = df_lut2['ISO']
lut2['continent'] = len(lut2)*[str(-999)]

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
        elevations.append(-9999)
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
    'source_code2',
    'source_lut',
    'iso-3166',
    'continent',
    ])
lut3['station_code'] = df_lut3['cru_code']
lut3['station_lat'] = [ int(np.round(df_lut3['lat'][i]*10**coords_dp)) for i in range(len(lut3)) ]
lut3['station_lon'] = [ int(np.round(df_lut3['lon'][i]*10**coords_dp)) for i in range(len(lut3)) ]
lut3['station_elevation'] = df_lut3['elev']
lut3['station_name'] = df_lut3['station_name']
lut3['station_country'] = df_lut3['country']
lut3['station_firstyear'] = df_lut3['Temp_start_year']
lut3['station_lastyear'] = df_lut3['Temp_end_year']
lut3['source_code1'] = df_lut3['station_id']
lut3['source_code2'] = len(df_lut3['station_id'])*[str(-999)]
lut3['source_lut'] = len(lut3)*[3]
lut3['iso-3166'] = len(lut3)*[str(-999)]
lut3['continent'] = len(lut3)*[str(-999)]
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
    'source_code2',
    'source_lut',
    'iso-3166',
    'continent',
    ])

stationcodes = df_lut4['stationcode'].unique()
lats = df_lut4.groupby('stationcode').mean()['stationlat'].values
lons = df_lut4.groupby('stationcode').mean()['stationlon'].values
elevations = df_lut4.groupby('stationcode').mean()['stationelevation'].values
stationlats = pd.Series(lats*10**coords_dp).replace(np.nan,-999)
stationlons = pd.Series(lons*10**coords_dp).replace(np.nan,-9999)
stationelevations = pd.Series(elevations).replace(np.nan,-9999)
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
lut4['source_lut'] = len(lut4)*[4]
lut4['iso-3166'] = len(lut4)*[str(-999)]
lut4['continent'] = len(lut4)*[str(-999)]
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
# DEDUCE: ISO 3166 alpha-2 codes (from country names)
#-----------------------------------------------------------------------------

print('loading country look-up tables ...')

COUNTRY_TO_ALPHA2 = {
    'Abkhazia': 'AB',
    'Afghanistan': 'AF',
    'Åland Islands': 'AX',
    'Albania': 'AL',
    'Algeria': 'DZ',
    'American Samoa': 'AS',
    'Andorra': 'AD',
    'Angola': 'AO',
    'Anguilla': 'AI',
    'Antigua and Barbuda': 'AG',
    'Argentina': 'AR',
    'Armenia': 'AM',
    'Aruba': 'AW',
    'Australia': 'AU',
    'Austria': 'AT',
    'Azerbaijan': 'AZ',
    'Bahamas': 'BS',
    'Bahrain': 'BH',
    'Bangladesh': 'BD',
    'Barbados': 'BB',
    'Belarus': 'BY',
    'Belgium': 'BE',
    'Belize': 'BZ',
    'Benin': 'BJ',
    'Bermuda': 'BM',
    'Bhutan': 'BT',
    'Bolivia': 'BO',
    'Bonaire': 'BQ',
    'Bosnia and Herzegovina': 'BA',
    'Botswana': 'BW',
    'Bouvet Island': 'BV',
    'Brazil': 'BR',
    'British Indian Ocean Territory': 'IO',
    'British Virgin Islands': 'VG',
    'Virgin Islands, British': 'VG',
    'Brunei': 'BN',
    'Brunei Darussalam': 'BN',
    'Bulgaria': 'BG',
    'Burkina Faso': 'BF',
    'Burundi': 'BI',
    'Cambodia': 'KH',
    'Cameroon': 'CM',
    'Canada': 'CA',
    'Cape Verde': 'CV',
    'Cayman Islands': 'KY',
    'Central African Republic': 'CF',
    'Chad': 'TD',
    'Chile': 'CL',
    'China': 'CN',
    'Christmas Island': 'CX',
    'Cocos (Keeling) Islands': 'CC',
    'Colombia': 'CO',
    'Comoros': 'KM',
    'Congo': 'CG',
    'Congo, Republic of': 'CG',
    'Republic of the Congo': 'CG',
    'Cook Islands': 'CK',
    'Costa Rica': 'CR',    
    "Cote-D'Ivoir": 'CI',
    'Croatia': 'HR',
    'Cuba': 'CU',
    'Curaçao': 'CW',
    'Cyprus': 'CY',
    'Czech Republic': 'CZ',
    'Congo, Democratic Republic of': 'CD',
    'Democratic Kampuchea': 'KH',    
    'Dem.P.Rep.Kor': 'KP',    
    'Democratic Republic of the Congo': 'CD',
    'Denmark': 'DK',
    'Djibouti': 'DJ',
    'Dominica': 'DM',
    'Dominican Republic': 'DO',
    'East Timor': 'TP',
    'Ecuador': 'EC',
    'Egypt': 'EG',
    'El Salvador': 'SV',
    'England': 'GB',
    'Equatorial Guinea': 'GQ',
    'Eritrea': 'ER',
    'Estonia': 'EE',
    'Ethiopia': 'ET',
    'Falkland Islands': 'FK',
    'Faroe Islands': 'FO',
    'Fiji': 'FJ',
    'Finland': 'FI',
    'France': 'FR',
    'French Guiana': 'GF',
    'French Polynesia': 'PF',
    'Gabon': 'GA',
    'Gambia': 'GM',
    'Georgia': 'GE',
    'Germany': 'DE',
    'Ghana': 'GH',
    'Gibraltar': 'GI',
    'Greece': 'GR',
    'Greenland': 'GL',
    'Grenada': 'GD',
    'Guadeloupe': 'GP',
    'Great Britain': 'GB',
    'Guam': 'GU',
    'Guatemala': 'GT',
    'Guernsey': 'GG',
    'Guinea': 'GN',
    'Guinea-Bissau': 'GW',
    'Guyana': 'GY',
    'Haiti': 'HT',
    'Heard Island and McDonald Islands': 'HM',
    'Honduras': 'HN',
    'Hong Kong': 'HK',
    'Hungary': 'HU',
    'Iceland': 'IS',
    'India': 'IN',
    'Indonesia': 'ID',
    'Iran': 'IR',
    'Iraq': 'IQ',
    'Ireland': 'IE',
    'Isle of Man': 'IM',
    'Islamic Republic of Iran': 'IR',
    'Israel': 'IL',
    'Italy': 'IT',
    'Ivory Coast': 'CI',
    'Jamaica': 'JM',
    'Japan': 'JP',
    'Jersey': 'JE',
    'Jordan': 'JO',
    'Kampuchea': 'KH',    
    'Kazakhstan': 'KZ',
    'Kenya': 'KE',
    "Korea, Democratic People's Republic of": 'KP',
    'Kiribati': 'KI',
    'Korea, Republic Of': 'KR',
    'Korea, North': 'KP',
    'Kosovo': 'XK',
    'Kuwait': 'KW',
    'Kyrgyzstan': 'KG',
    'Laos': 'LA',
    "Lao People's Democratic Republic": 'LA',
    "Lao P.D.R.": 'LA',        
    'Latvia': 'LV',
    'Lebanon': 'LB',
    'Lesotho': 'LS',
    'Liberia': 'LR',
    'Libya': 'LY',
    'Liechtenstein': 'LI',
    'Lithuania': 'LT',
    'Luxembourg': 'LU',
    'Macau': 'MO',
    'Macedonia': 'MK',
    'Macedonia, The Former Yugoslav Republic Of': 'MK',
    'Madagascar': 'MG',
    'Malawi': 'MW',
    'Malaysia': 'MY',
    'Maldives': 'MV',
    'Mali': 'ML',
    'Malta': 'MT',
    'Marshall Islands': 'MH',
    'Martinique': 'MQ',
    'Mauritania': 'MR',
    'Mauritius': 'MU',
    'Mayotte': 'YT',
    'Mexico': 'MX',
    'Micronesia': 'FM',
    'Micronesia, Federated States of': 'FM',
    'Moldova': 'MD',
    'Moldova, Republic Of': 'MD',
    'Monaco': 'MC',
    'Mongolia': 'MN',
    'Montenegro': 'ME',
    'Montserrat': 'MS',
    'Morocco': 'MA',
    'Mozambique': 'MZ',
    'Myanmar': 'MM',
    'Namibia': 'NA',
    'Nauru': 'NR',
    'Nepal': 'NP',
    'Netherlands': 'NL',
    'Neth. Antille': 'NL',    
    'New Caledonia': 'NC',
    'New Zealand': 'NZ',
    'Nicaragua': 'NI',
    'Niger': 'NE',
    'Nigeria': 'NG',
    'Niue': 'NU',
    'Norfolk Island': 'NF',
    'North Korea': 'KP',
    'Northern Cyprus': 'CY',
    'Northern Mariana Islands': 'MP',
    'Norway': 'NO',
    'Ocean Is(BR).': 'BR',    
    'Ocean Is(FR).': 'FR',    
    'Oman': 'OM',      
#    'Pacific Oc.': 'XX',       
    'Pacific Oc.': 'XX',       
    'Pacific (US)': 'US',    
    'Pacific (Us)': 'US',    
    'Pakistan': 'PK',
    'Palau': 'PW',
    'Palestine': 'PS',
    'Panama': 'PA',
    'Papua New Guinea': 'PG',
    'Paraguay': 'PY',
    'Peru': 'PE',
    'Philippines': 'PH',
    'Poland': 'PL',
    'Portugal': 'PT',
    'Puerto Rico': 'PR',
    'Qatar': 'QA',
    'Romania': 'RO',
    'Russia': 'RU',
    'Russian Federation': 'RU',
    'Rwanda': 'RW',
    'Réunion': 'RE',
    'Saba': 'BQ',
    'Saint Barthélemy': 'BL',
    'Saint Helena, Ascension and Tristan da Cunha': 'SH',    
    'St.Helena(BR)': 'SH',
    'St.Helena (BR': 'SH',    
    'Saint Kitts and Nevis': 'KN',
    'St. Kitts and Nevis': 'KN',
    'Saint Lucia': 'LC',
    'St. Lucia': 'LC',
    'Saint Martin': 'MF',
    'St. Martin': 'MF',
    'Saint Pierre and Miquelon': 'PM',
    'St. Pierre and Miquelon': 'PM',
    'Saint Vincent and the Grenadines': 'VC',
    'St. Vincent and The Grenadines': 'VC',
    'Samoa': 'WS',
    'San Marino': 'SM',
    'Saudi Arabia': 'SA',
    'Scotland': 'GB',
    'Senegal': 'SN',
    'Serbia': 'RS',
    'Seychelles': 'SC',
    'Sierra Leone': 'SL',
    'Singapore': 'SG',
    'Sint Eustatius': 'BQ',
    'Slovakia': 'SK',
    'Slovenia': 'SI',
    'Solomon Islands': 'SB',
    'Somalia': 'SO',
    'Somaliland': 'SO',
    'South Africa': 'ZA',
    'South Georgia and the South Sandwich Islands': 'GS',
    'South Korea': 'KR',
    'South Ossetia': 'OS',
    'South Sudan': 'SS',
    'Spain': 'ES',
    'Sri Lanka': 'LK',
    'Sudan': 'SD',
    'Suriname': 'SR',
    'Svalbard': 'SJ',
    'Swaziland': 'SZ',
    'Sweden': 'SE',
    'Switzerland': 'CH',
    'Syria': 'SY',
    'Syrian Arab Republic': 'SY',
    'São Tomé and Príncipe': 'ST',
    'Sao Tome-And': 'ST',    
    'Taiwan': 'TW',
    'Taiwan, Province of China': 'TW',
    'Tajikistan': 'TJ',
    'Tanzania': 'TZ',
    'Tanzania, United Republic Of': 'TZ',
    'Thailand': 'TH',
    'Togo': 'TG',
    'Tokelau': 'TK',
    'Tonga': 'TO',
    'Trinidad and Tobago': 'TT',
    'Tunisia': 'TN',
    'Turkey': 'TR',
    'Turkmenistan': 'TM',
    'Turks and Caicos Islands': 'TC',
    'Turks and Caicos': 'TC',
    'Tuvalu': 'TV',
    'Uganda': 'UG',
    'Ukraine': 'UA',
    'Uk': 'GB', # Added to handle abbreviated country name
    'UK': 'GB', # Added to handle abbreviated country name
    'United Kingdom': 'GB',
    'United Arab Emirates': 'AE',
    'U.A.E.': 'AE',    # Added to handle abbreviated country name
    'United States Virgin Islands': 'VI',    
    'United States': 'US',
    'United States of America': 'US',
    'Uruguay': 'UY',
    'Us': 'US', # Added to handle abbreviated country name
    'US': 'US', # Added to handle abbreviated country name
    'Usa': 'US', # Added to handle abbreviated country name
    'USA': 'US', # Added to handle abbreviated country name
    'Ussr': 'RU', # Added to handle abbreviated country name
    'USSR': 'RU', # Added to handle abbreviated country name
    'Uzbekistan': 'UZ',
    'Vanuatu': 'VU',
    'Venezuela': 'VE',
    'Vietnam': 'VN',
    'Wales': 'GB',
    'Wallis and Futuna': 'WF',
    'Yemen': 'YE',
    'Zambia': 'ZM',
    'Zimbabwe': 'ZW',
}

#-----------------------------------------------------------------------------
# DEDUCE: continent (from alpha-2 codes)
#-----------------------------------------------------------------------------

ALPHA2_TO_CONTINENT = {
    'AB': 'Asia',
    'AD': 'Europe',
    'AE': 'Asia',
    'AF': 'Asia',
    'AG': 'North America',
    'AI': 'North America',
    'AL': 'Europe',
    'AM': 'Asia',
    'AO': 'Africa',
    'AR': 'South America',
    'AS': 'Oceania',
    'AT': 'Europe',
    'AU': 'Oceania',
    'AW': 'North America',
    'AX': 'Europe',
    'AZ': 'Asia',
    'BA': 'Europe',
    'BB': 'North America',
    'BD': 'Asia',
    'BE': 'Europe',
    'BF': 'Africa',
    'BG': 'Europe',
    'BH': 'Asia',
    'BI': 'Africa',
    'BJ': 'Africa',
    'BL': 'North America',
    'BM': 'North America',
    'BN': 'Asia',
    'BO': 'South America',
    'BQ': 'North America',
    'BR': 'South America',
    'BS': 'North America',
    'BT': 'Asia',
    'BV': 'Antarctica',
    'BW': 'Africa',
    'BY': 'Europe',
    'BZ': 'North America',
    'CA': 'North America',
    'CC': 'Asia',
    'CD': 'Africa',
    'CF': 'Africa',
    'CG': 'Africa',
    'CH': 'Europe',
    'CI': 'Africa',
    'CK': 'Oceania',
    'CL': 'South America',
    'CM': 'Africa',
    'CN': 'Asia',
    'CO': 'South America',
    'CR': 'North America',
    'CU': 'North America',
    'CV': 'Africa',
    'CW': 'North America',
    'CX': 'Asia',
    'CY': 'Asia',
    'CZ': 'Europe',
    'DE': 'Europe',
    'DJ': 'Africa',
    'DK': 'Europe',
    'DM': 'North America',
    'DO': 'North America',
    'DZ': 'Africa',
    'EC': 'South America',
    'EE': 'Europe',
    'EG': 'Africa',
    'ER': 'Africa',
    'ES': 'Europe',
    'ET': 'Africa',
    'FI': 'Europe',
    'FJ': 'Oceania',
    'FK': 'South America',
    'FM': 'Oceania',
    'FO': 'Europe',
    'FR': 'Europe',
    'GA': 'Africa',
    'GB': 'Europe',
    'GD': 'North America',
    'GE': 'Asia',
    'GF': 'South America',
    'GG': 'Europe',
    'GH': 'Africa',
    'GI': 'Europe',
    'GL': 'North America',
    'GM': 'Africa',
    'GN': 'Africa',
    'GP': 'North America',
    'GQ': 'Africa',
    'GR': 'Europe',
    'GS': 'South America',
    'GT': 'North America',
    'GU': 'Oceania',
    'GW': 'Africa',
    'GY': 'South America',
    'HK': 'Asia',
    'HM': 'Antarctica',
    'HN': 'North America',
    'HR': 'Europe',
    'HT': 'North America',
    'HU': 'Europe',
    'ID': 'Asia',
    'IE': 'Europe',
    'IL': 'Asia',
    'IM': 'Europe',
    'IN': 'Asia',
    'IO': 'Asia',
    'IQ': 'Asia',
    'IR': 'Asia',
    'IS': 'Europe',
    'IT': 'Europe',
    'JE': 'Europe',
    'JM': 'North America',
    'JO': 'Asia',
    'JP': 'Asia',
    'KE': 'Africa',
    'KG': 'Asia',
    'KH': 'Asia',
    'KI': 'Oceania',
    'KM': 'Africa',
    'KN': 'North America',
    'KP': 'Asia',
    'KR': 'Asia',
    'KW': 'Asia',
    'KY': 'North America',
    'KZ': 'Asia',
    'LA': 'Asia',
    'LB': 'Asia',
    'LC': 'North America',
    'LI': 'Europe',
    'LK': 'Asia',
    'LR': 'Africa',
    'LS': 'Africa',
    'LT': 'Europe',
    'LU': 'Europe',
    'LV': 'Europe',
    'LY': 'Africa',
    'MA': 'Africa',
    'MC': 'Europe',
    'MD': 'Europe',
    'ME': 'Europe',
    'MF': 'North America',
    'MG': 'Africa',
    'MH': 'Oceania',
    'MK': 'Europe',
    'ML': 'Africa',
    'MM': 'Asia',
    'MN': 'Asia',
    'MO': 'Asia',
    'MP': 'Oceania',
    'MQ': 'North America',
    'MR': 'Africa',
    'MS': 'North America',
    'MT': 'Europe',
    'MU': 'Africa',
    'MV': 'Asia',
    'MW': 'Africa',
    'MX': 'North America',
    'MY': 'Asia',
    'MZ': 'Africa',
    'NA': 'Africa',
    'NC': 'Oceania',
    'NE': 'Africa',
    'NF': 'Oceania',
    'NG': 'Africa',
    'NI': 'North America',
    'NL': 'Europe',
    'NO': 'Europe',
    'NP': 'Asia',
    'NR': 'Oceania',
    'NU': 'Oceania',
    'NZ': 'Oceania',
    'OM': 'Asia',
    'OS': 'Asia',
    'PA': 'North America',
    'PE': 'South America',
    'PF': 'Oceania',
    'PG': 'Oceania',
    'PH': 'Asia',
    'PK': 'Asia',
    'PL': 'Europe',
    'PM': 'North America',
    'PR': 'North America',
    'PS': 'Asia',
    'PT': 'Europe',
    'PW': 'Oceania',
    'PY': 'South America',
    'QA': 'Asia',
    'RE': 'Africa',
    'RO': 'Europe',
    'RS': 'Europe',
    'RU': 'Europe',
    'RW': 'Africa',
    'SA': 'Asia',
    'SB': 'Oceania',
    'SC': 'Africa',
    'SD': 'Africa',
    'SE': 'Europe',
    'SG': 'Asia',
    'SH': 'Africa',
    'SI': 'Europe',
    'SJ': 'Europe',
    'SK': 'Europe',
    'SL': 'Africa',
    'SM': 'Europe',
    'SN': 'Africa',
    'SO': 'Africa',
    'SR': 'South America',
    'SS': 'Africa',
    'ST': 'Africa',
    'SV': 'North America',
    'SY': 'Asia',
    'SZ': 'Africa',
    'TC': 'North America',
    'TD': 'Africa',
    'TG': 'Africa',
    'TH': 'Asia',
    'TJ': 'Asia',
    'TK': 'Oceania',
    'TM': 'Asia',
    'TN': 'Africa',
    'TO': 'Oceania',
    'TP': 'Asia',
    'TR': 'Asia',
    'TT': 'North America',
    'TV': 'Oceania',
    'TW': 'Asia',
    'TZ': 'Africa',
    'UA': 'Europe',
    'UG': 'Africa',
    'US': 'North America',
    'UY': 'South America',
    'UZ': 'Asia',
    'VC': 'North America',
    'VE': 'South America',
    'VG': 'North America',
    'VI': 'North America',
    'VN': 'Asia',
    'VU': 'Oceania',
    'WF': 'Oceania',
    'WS': 'Oceania',
    'XK': 'Europe',
    'XX': 'Oceania',
    'YE': 'Asia',
    'YT': 'Africa',
    'ZA': 'Africa',
    'ZM': 'Africa',
    'ZW': 'Africa',
}

country_list = [ list(COUNTRY_TO_ALPHA2)[i] for i in range(len(COUNTRY_TO_ALPHA2)) ]
alpha2_list = [ COUNTRY_TO_ALPHA2[list(COUNTRY_TO_ALPHA2)[i]] for i in range(len(COUNTRY_TO_ALPHA2)) ]

#-----------------------------------------------------------------------------
# FIX: Country names
#-----------------------------------------------------------------------------

print('fixing country names ...')

lut['station_country'] = lut['station_country'].str.rstrip("-")
station_country_fix = lut.station_country.apply(strip_character)
station_country_list = []
for i in range(len(lut)):
    c = station_country_fix[i]
    m = difflib.get_close_matches( c.title(), country_list, n=1, cutoff=0.5)        
    if len(m) == 0:
        a = '-999'
    else:
        a = m[0]
    station_country_list.append(a)

#-----------------------------------------------------------------------------
# DEDUCE: ISO 3166-1 alpha-2 codes
#-----------------------------------------------------------------------------

print('deducing ISO 3166 alpha-2 codes ...')

# https://towardsdatascience.com/matching-country-information-in-python-7e1928583644
# https://towardsdatascience.com/using-python-to-create-a-world-map-from-a-list-of-country-names-cd7480d03b10    
#c = lut.station_country[0]
#x = pycountry.countries.search_fuzzy(c)[0].alpha_2
#x = pycountry.countries.get(alpha_2='GB').name

#station_alpha2_list = [ pycountry.countries.search_fuzzy(lut.station_country[i])[0].alpha_2 for i in range(len(lut)) ] 
#station_alpha2_list = [ COUNTRY_TO_ALPHA2[station_country_list[i]] for i in range(len(lut)) ]
station_alpha2_list = []
for i in range(len(lut)):
    c = station_country_list[i]
    if c == '-999':
        a = '-999'
    else:
        a = COUNTRY_TO_ALPHA2[station_country_list[i]]
    station_alpha2_list.append(a)

#-----------------------------------------------------------------------------
# DEDUCE: continents 
#-----------------------------------------------------------------------------

print('deducing continents ...')

station_continent_list = []
for i in range(len(lut)):
    c = station_country_list[i]
    if c == '-999':
        a = '-999'
    else:
        a = ALPHA2_TO_CONTINENT[station_alpha2_list[i]]
    station_continent_list.append(a)

lut['station_country'] = station_country_list
lut['iso-3166'] = station_alpha2_list
lut['continent'] = station_continent_list

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
