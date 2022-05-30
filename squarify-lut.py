#!/usr/bin/env python

#-----------------------------------------------------------------------
# PROGRAM: squarify-lut.py
#-----------------------------------------------------------------------
# Version 0.2
# 30 May, 2022
# Dr Michael Taylor
# https://patternizer.github.io
# patternizer AT gmail DOT com
# michael DOT a DOT taylor AT uea DOT ac DOT uk
#-----------------------------------------------------------------------

# Dataframe libraries:
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
#sns.set_theme(style='darkgrid', palette='deep', font='sans-serif', font_scale=1.2, color_codes=True, rc=None)
sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})
import plotly.express as px
import squarify 
# import chord 

#-----------------------------------------------------------------------------
# PACKAGE VERSIONS
#-----------------------------------------------------------------------------

import platform
print("python       : ", platform.python_version())
print("pandas       : ", pd.__version__)
print("matplotlib   : ", matplotlib.__version__)
print("seaborn      : ", sns.__version__)
print("squarify     :  0.4.3")
                              
#-----------------------------------------------------------------------------
# SETTINGS
#-----------------------------------------------------------------------------

data_dir = 'DATA/'
output_dir = 'OUT/'

fontsize = 16
nlabels = 50
colors = 'Shikari_lite'

if colors == 'Viridis':    
    colors = px.colors.sequential.Viridis_r
elif colors == 'Cividis':    
    colors = px.colors.sequential.Cividis_r
elif colors == 'Plotly3':
    colors = px.colors.sequential.Plotly3_r
elif colors == 'Magma':
    colors = px.colors.sequential.Magma_r
elif colors == 'Shikari_lite':
    colors = ['#d8d7d5','#a1dcfc','#fdee03','#75b82b','#a84190','#0169b3']
elif colors == 'Shikari_dark':
    colors = ['#2f2f2f','#a1dcfc','#fdee03','#75b82b','#a84190','#0169b3']      

glosat_version = 'GloSAT.p04'
    
#-----------------------------------------------------------------------------
# LOAD: look-up table (LUT)
#-----------------------------------------------------------------------------

print('loading look-up table ...')

lut = pd.read_csv( output_dir + 'lut.csv')
lut.dropna(inplace=True)

#-----------------------------------------------------------------------------
# PLOT: treemap of continent Nyrs in GloSAT
#-----------------------------------------------------------------------------

continents = lut[lut['source_lut']==4].groupby('continent').count()['Nyr'].sort_values(ascending=False)[:nlabels]

sizes = continents
labels = ["%s\n%s" % (label) for label in zip(continents.index,continents)] #labels = continents.index.tolist()

fig,ax = plt.subplots(figsize=(15,10))
plt.title('N(stations) by continent in GloSAT', fontsize=fontsize, fontweight='bold')
squarify.plot(sizes=sizes, label=labels, alpha=1.0, color=colors)
plt.axis('off')
plt.savefig('squarify-continents-glosat.png', dpi=300, bbox_inches='tight')
plt.close('all')

#-----------------------------------------------------------------------------
# PLOT: treemap of continent Nyrs in GloSAT
#-----------------------------------------------------------------------------

#countries = lut[lut['source_lut']==4].groupby('iso-3166').sum()['Nyr'].sort_values(ascending=False)[:nlabels]
#countries = lut[lut['source_lut']==4].dropna().groupby('iso-3166').count()['Nyr'].sort_values(ascending=False)[:nlabels]
countries = lut[lut['source_lut']==4].dropna().groupby('station_country').count()['Nyr'].sort_values(ascending=False)[:nlabels]

sizes = countries
labels = ["%s\n%s" % (label) for label in zip(countries.index,countries)] #labels = countries.index.tolist()

fig,ax = plt.subplots(figsize=(15,10))
plt.title('N(stations) by country in ' + glosat_version, fontsize=fontsize, fontweight='bold')
squarify.plot(sizes=sizes, label=labels, alpha=1.0, color=colors)
plt.axis('off')
plt.savefig('squarify-countries-glosat.png', dpi=300, bbox_inches='tight')
plt.close('all')


#-----------------------------------------------------------------------------
print('** END')
