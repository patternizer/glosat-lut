#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: ncdc-reader.py
#-----------------------------------------------------------------------
# Version 0.1
# 9 June, 2022
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
nheader = 1

ncdc_file = 'DATA/NCDC/master_stn_hist.sort'
pkl_file = 'df_ncdc.pkl'

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

print('loading latest inventory ...')

# COLUMNS                      STATION INFORMATION COMPONENT
#_________     _____________________________________________________________
#
#   1 -   8     Station Id Number ( Internal Number Used by the HOMR Database )
#  10 -  11     Record Type ( Used for Archive Purposes )
#  13 -  18     Coop Station Id
#  20 -  21     Climate Division
#  23 -  27     WBAN Station Id
#  29 -  33     WMO Station Id
#  35 -  38     FAA LOC ID
#  40 -  44     NWS Location Identifier
#  46 -  49     ICAO Station Id
#  51 -  70     Country
#  72 -  73     State/Province Abbreviation ( United States/Canada Only )
#  75 - 104     County Name ( United States and Pacific Islands Only )
# 106 - 110     Time Zone ( Number of Hours Added to Local Time to Get Greenwich Time )
# 112 - 141     Historical Coop Station Name
# 143 - 172     Historical WBAN Station Name
# 174 - 181     Beginning Date of Period of Record ( YYYYMMDD ) ( "00000101" => Unknown Begin Date )
# 183 - 190     Ending Date of Period of Record ( YYYYMMDD ) ( "99991231" => Station Currently Open )
# 192 - 192     Latitude Direction ( " " => North, "-" => South )
# 193 - 194     Latitude Degrees
# 196 - 197     Latitude Minutes
# 199 - 200     Latitude Seconds
# 202 - 202     Longitude Direction ( " " => East, "-" => West )
# 203 - 205     Longitude Degrees
# 207 - 208     Longitude Minutes
# 210 - 211     Longitude Seconds
# 213 - 214     Latitude/Longitude Precision Code ( "53" => Degrees )
#               ( "54" => Degrees, Whole Minutes )
#               ( "55" => Degrees, Whole Minutes, Whole Seconds )
#               ( "56" => Decimal Degrees, to Tenths )
#               ( "57" => Decimal Degrees, to Hundredths )
#               ( "58" => Decimal Degrees, to Thousandths )
#               ( "59" => Decimal Degrees, to Ten Thousandths )
#               ( "60" => Decimal Degrees, to Hundred Thousandths )
#               ( "62" => Degrees, Decimal Minutes to Tenths )
#               ( "63" => Degrees, Decimal Minutes to Hundredths )
#               ( "64" => Degrees, Decimal Minutes to Thousandths )
#               ( "66" => Deg, Minutes, Decimal Seconds to Tenths )
#               ( "67" => Deg, Min, Decimal Seconds to Hundredths )
# 216 - 221     Elevation - Ground( Feet )
# 223 - 228     Elevation ( Feet )
# 230 - 231     Elevation Type Code
#               ( "0"  => Unknown Elevation Type )
#               ( "2"  => Barometer Ivory Point )
#               ( "6"  => Ground )
#               ( "7"  => Field, Airport or Runway )
#               ( "12" => Zero Datum of a River Gage )
# 233 - 243     Station Relocation
# 245 - 294     Station Types

f = open(ncdc_file)
lines = f.readlines()

station_ids = []
record_types = []
coop_station_ids = []
climate_divisions = []
wban_station_ids = []    
wmo_station_ids = []
faa_loc_ids = []
nws_location_identifiers = []
icao_station_ids = []
countries = []
states = []
counties = []
timezones = []
historical_coop_station_names = []
historical_wban_station_names = []
begin_dates = []
end_dates = []
latitude_directions = []
latitude_degrees = []
latitude_minutes = []
latitude_seconds = []
longitude_directions = []
longitude_degrees = []
longitude_minutes = []
longitude_seconds = []
latlon_precision_codes = []
elevation_above_grounds = []
elevations = []
elevation_type_codes = []
station_relocations = []
station_types = []

for i in range(len(lines)):

    if i > (nheader-1): # ignore header lines
        
        station_id = lines[i][0:8]
        record_type = lines[i][9:11]
        coop_station_id = lines[i][12:18]
        climate_division = lines[i][19:21]
        wban_station_id = lines[i][22:27]    
        wmo_station_id = lines[i][28:33]
        faa_loc_id = lines[i][34:38]
        nws_location_identifier = lines[i][39:44]
        icao_station_id = lines[i][45:49]
        country = lines[i][50:70]
        state = lines[i][71:73]
        county = lines[i][74:104]
        timezone = lines[i][105:110]
        historical_coop_station_name = lines[i][111:141]
        historical_wban_station_name = lines[i][142:172]
        begin_date = lines[i][173:181]
        end_date = lines[i][182:190]
        latitude_direction = lines[i][191:192]
        latitude_degree = lines[i][192:194]
        latitude_minute = lines[i][195:197]
        latitude_second = lines[i][198:200]
        longitude_direction = lines[i][201:202]
        longitude_degree = lines[i][202:205]
        longitude_minute = lines[i][206:208]
        longitude_second = lines[i][209:211]
        latlon_precision_code = lines[i][212:214]
        elevation_above_ground = lines[i][215:221]
        elevation = lines[i][222:228]
        elevation_type_code = lines[i][229:231]
        station_relocation = lines[i][232:243]
        station_type = lines[i][244:294]
            
        station_ids.append( station_id )
        record_types.append( record_type )
        coop_station_ids.append( coop_station_id )
        climate_divisions.append( climate_division )
        wban_station_ids.append( wban_station_id )
        wmo_station_ids.append( wmo_station_id )
        faa_loc_ids.append( faa_loc_id )
        nws_location_identifiers.append( nws_location_identifier )
        icao_station_ids.append( icao_station_id )
        countries.append( country )
        states.append( state )
        counties.append( county )
        timezones.append( timezone )
        historical_coop_station_names.append( historical_coop_station_name )
        historical_wban_station_names.append( historical_wban_station_name )
        begin_dates.append( begin_date )
        end_dates.append( end_date )
        latitude_directions.append( latitude_direction )
        latitude_degrees.append( latitude_degree )
        latitude_minutes.append( latitude_minute )
        latitude_seconds.append( latitude_second )
        longitude_directions.append( longitude_direction )
        longitude_degrees.append( longitude_degree )
        longitude_minutes.append( longitude_minute )
        longitude_seconds.append( longitude_second )
        latlon_precision_codes.append( latlon_precision_code )
        elevation_above_grounds.append( elevation_above_ground )
        elevations.append( elevation )
        elevation_type_codes.append( elevation_type_code )
        station_relocations.append( station_relocation )
        station_types.append( station_type )

f.close()
    
df_ncdc = pd.DataFrame({
                                
    'station_id':station_ids,
    'record_type':record_types,
    'coop_station_id':coop_station_ids,
    'climate_division':climate_divisions,
    'wban_station_id':wban_station_ids,
    'wmo_station_id':wmo_station_ids,
    'faa_loc_id':faa_loc_ids,
    'nws_location_identifier':nws_location_identifiers,
    'icao_station_id':icao_station_ids,
    'country':countries,
    'state':states,
    'county':counties,
    'timezone':timezones,
    'historical_coop_station_name':historical_coop_station_names,
    'historical_wban_station_name':historical_wban_station_names,
    'begin_date':begin_dates,
    'end_date':end_dates,
    'latitude_direction':latitude_directions,
    'latitude_degree':latitude_degrees,
    'latitude_minute':latitude_minutes,
    'latitude_second':latitude_seconds,
    'longitude_direction':longitude_directions,
    'longitude_degree':longitude_degrees,
    'longitude_minute':longitude_minutes,
    'longitude_second':longitude_seconds,
    'latlon_precision_code':latlon_precision_codes,
    'elevation_above_ground':elevation_above_grounds,
    'elevation':elevations,
    'elevation_type_code':elevation_type_codes,
    'station_relocation':station_relocations,
    'station_type':station_type
            
    })   
    
df_ncdc.to_pickle( pkl_file, compression='bz2' )
        
#------------------------------------------------------------------------------
print('** END')

