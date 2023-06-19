![image](https://github.com/patternizer/glosat-lut/blob/master/PLOTS/glosat-inventory-nyr-descending.png)
![image](https://github.com/patternizer/glosat-lut/blob/master/PLOTS/location_map_glosat.png)

# glosat-lut

Look up table generator for merged inventories from GloSAT and C3S colleagues as part of ongoing work for the [GloSAT](https://www.glosat.org) project: www.glosat.org. 

## Contents

* `glosat-lut.py` - python script to read in inventories and coerce their data in a common format compatible with CRUTEM5 station files
* `plot-station-months-map.py` - python script to plot GloSAT and C3S stations in the early record 1781-1850
* `glosat-c3s-crutem-comparison.py` - python script to compare GloSAT and C3S station data for a single stationcode
* `glosat-c3s-crutem-converter.py` - python script to load and convert processed monthly data to CRUTEM5 station file format
* `glosat-c3s-crutem-converter-raw.py` - python script to load and convert unprocessed raw data to CRUTEM5 station file format

## Instructions for use

The first step is to clone the latest glosat-lut code and step into the installed Github directory: 

    $ git clone https://github.com/patternizer/glosat-lut.git
    $ cd glosat-lut

Then create a DATA/ directory and copy to it the required inventories listed in python glosat-lut.py.

### Using Standard Python

The code is designed to run in an environment using Miniconda3-latest-Linux-x86_64.

    $ python glosat-lut.py

This will generate a comma separated value (CSV) look-up table (LUT) in the output directory OUT/ for each inventory with scaled integer format (lat and lon variables are x10) as well as variants sorted by number of years of monthly observations, first year of recorded obervation and station name.

## License

The code is distributed under terms and conditions of the [Open Government License](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Contact information

* [Michael Taylor](michael.a.taylor@uea.ac.uk)

