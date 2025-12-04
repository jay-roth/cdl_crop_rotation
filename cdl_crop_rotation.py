# -*- coding: utf-8 -*-
"""
Created on Thu Dec  4 09:48:54 2025

@author: Jason.Roth
@title: Water Resources Engineer
@affiliation: USDA-NRCS MN SO
@email:jason.roth@usda.gov

Pulls Cropland data layer raster for an AOI shapefile and range of years
then calculates the dominant crop/cover within the AOI for each year and stores 
results to a csv
"""
import os
import requests as req
import xml.etree.ElementTree as et
import rasterio as rio
import rasterio.mask as mask
import numpy as np
import geopandas as gpd
import pandas as pd

#### USER Defined variables ###################################################
## if true, will delete CDL raster after computation is done
clean_up = False

## shape file to mask raster in assessing dominant crop, this is fld bdry/AOI
clip_file = 'test_area.shp'

## beginning year
beg_yr = 2011

## number of years to fetch data for
num_yrs = 4

#### END USER Defined variables, edit below at your own risk ##################

## set working dir
cwd = os.getcwd()

## dirs required for structure
dirs = ['geometry', 'results', 'raster']

## CDL webservice endpoint
base_url = 'https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile'

## csv containing cdl codes, names and rgb vals
cmap_file = 'cdl_map.csv'

## container for results
results = []

## error switch
err = False

cdl_yrs = [2009, 2024]

## check if the proposed timeframe and number of years is with CDL params
if num_yrs > cdl_yrs[1] - cdl_yrs[0]:
    num_yrs = cdl_yrs[1] - cdl_yrs[0]

if beg_yr < cdl_yrs[0]:
    beg_yr = cdl_yrs[0]
    
if beg_yr > cdl_yrs[1]:
    beg_yr = cdl_yrs[1] - num_yrs
    
## check directory structure
for d in dirs:
    if not os.path.exists(os.path.join(cwd, d)):
        ## needs to have a geometry to clip from in the geometry folder
        if d == dirs[0]:
            err = True
        ## make some dirs to store stuff in
        else:
            os.makedirs(os.path.join(cwd, d))
            
if not os.path.exists(os.path.join(cwd, 'geometry', clip_file)): 
    err = True           

if err:
    print("""Geometry directory or AOI shape file not found.
             Ensure working directory has a geometry directory with the
             specified shapefile""")

else:
    ## read in the cdl map
    cmap = pd.read_csv(os.path.join(cwd, 'cdl_map.csv'))

    ## read in geometry
    clip_shp = gpd.read_file(os.path.join(cwd, 'geometry', clip_file))

    ## Coords must be 5070, Albers Equal Area Conic
    clip_shp = clip_shp.to_crs("EPSG:5070")

    ## set bounding box values for the geometry
    bb = [int(b) for b in clip_shp.loc[0].geometry.bounds]
    
    ## loop over years 
    for yr in [beg_yr+i for i in range(num_yrs)]:
        
        print("processing data for {0}".format(yr))
        
        ## format bounding box string for api params
        bb_str = '{0},{1},{2},{3}'.format(*bb)
        
        ## dict for api params
        params = {'year':yr, 'bbox':bb_str}
        
        ## tell api to clip some data for our bounding box and store it on the server
        dat = req.get('https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile', params)
        
        ## get the url of the stored data
        root = et.fromstring(dat.content)
        dl_url = root.findtext(".//returnURL")
        
        ## download the stored data (raster for our area and this year)
        dat = req.get(dl_url)
        
        ## format bb string for file storage (i don't like commas in file names)
        bb_str = '{0}_{1}_{2}_{3}'.format(*bb)
        
        ## make a path to store the CDL data for this year and this bounding box
        out_path  = os.path.join(os.getcwd(), 'raster', "cdl_{0}_{1}.tif".format(yr, bb_str))
        
        ## delete data if it exists, could save it but refresh it in case old is corrupt
        if os.path.exists(out_path):
            os.remove(out_path)
            
        ## write the cdl raster
        with open(out_path, 'wb') as dest:
            dest.write(dat.content)
            dest.close()
            
        ## reopen it for reading and analysis
        with rio.open(out_path, 'r') as ras:
            
            #mask it to the actual extent of the polygon not the bounding box
            dat, xfm = mask.mask(ras, clip_shp.geometry, crop=True)
            
            ## close the raster
            ras.close()
            
            ## delete the cdl raster if we don't need it any longer
            if clean_up:
                os.remove(out_path)
                
            ## get unique cell values
            crops = np.unique(dat[np.where(dat!=0)])
            
            ## instantiate some vars to keep track of dominant cover
            crop_max = 0
            max_cnt = 0
            
            ## loop over each crop in the raster
            for c in crops:
                
                ## get number of cells with this crop in our AOI polygon
                cnt = np.sum(np.where(dat==c,1,0))
                
                ## check if this crop comprises a larger area than the prior max 
                if cnt > max_cnt:
                    crop_max = c
                    max_cnt = cnt
                    
            ## get the name (string) of the crop
            crop_name = cmap.loc[cmap.Codes==crop_max]['Current Class Names'].values[0]
            
            ## store the year and the crop to
            results.append([yr, crop_name])
            
    ## store all results to a data frame
    results = pd.DataFrame(results, columns=['year', 'crop'])
    
    ## path to store csv of results
    out_path  = os.path.join(cwd, 'results', "rotation_{0}_{1}_{2}.csv".format(beg_yr, yr, bb_str))
    
    ## remove prior output just in case its corrupt
    if os.path.exists(out_path):
        os.remove(out_path)
        
    ## write csv
    results.to_csv(out_path, index=False) 