# -*- coding: utf-8 -*-
"""
Created on Thu Dec  4 09:48:54 2025

@author: Jason.Roth
@title: Water Resources Engineer
@affiliation: USDA-NRCS MN SO
@email:jason.roth@usda.gov

Pulls Cropland data layer raster for an AOI shapefile and range of years
then calculates the dominant crop/cover within the AOI for each year and stores 
results to a csv.

User points script to a shapefile (clip_file) located with a directory
named "geometry" within the working directory. The shapefile should contain
a single polygon representing the AOI for the crop/cover rotation.

"""
import os
import requests as req
import xml.etree.ElementTree as et
import rasterio as rio
import rasterio.mask as mask
import numpy as np
import geopandas as gpd
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib import pyplot as plt
import matplotlib as mpl
from mpl_toolkits.axes_grid1 import make_axes_locatable
from rasterio.features import rasterize
import matplotlib.ticker as mtick

def calc_fig_panels(n):
    
    if n == 1:
        ncols = 1
        nrows = 1
    elif n == 2:
        ncols = 1
        nrows = 2        
    elif n == 3:
        ncols = 1
        nrows = 3
    else:
        nrows = 3
        ncols = 2
        
    return nrows, ncols

def make_colormap(vals, cmapdf, norm=True, offset=0.001):

    """
    function to create a unique color map for values contained in a grid.
    based on rbg values specified in the CDL_Legend.htm 
    """
    
    if len(vals)>0:
        ## drop the values that are not contained in this grid
        cmapdf = cmapdf.loc[vals]

    ## some containers to store some variables in
    clrs = []
    bnds = [] 
    labs = []
    tiks = []
    bnds = [-1]
    lb = -1
    
    ## iterate over remaining values and create a list of RGB values that correspond to each landuse
    for i in cmapdf.index:
        ## normalize RGB to 255, matplotlib requires decimal vals
        r = cmapdf.loc[i,'ESRI Red']/255
        g = cmapdf.loc[i,'ESRI Green']/255
        b = cmapdf.loc[i,'ESRI Blue']/255
        clrs.append((r,g,b))
        labs.append(cmapdf.loc[i,'Current Class Names'])
        bnds.append(i+offset)
        tiks.append((i+lb)/2.)
        lb = i
        
    # make colormap object
    cmap = mpl.colors.ListedColormap(clrs, 'cdl')
    if norm == True:
        norm = mpl.colors.BoundaryNorm(bnds, cmap.N)
    else:
        norm = 1
        
    cmap.set_under((1,1,1,0))
    cmap.set_over((1,1,1,0))
    
    return(cmap, norm, tiks, labs)

def get_cdl_stats(inras, cmap):
    
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


#### USER Defined variables ###################################################
## if true, will delete CDL raster after computation is done
clean_up = False

## shape file to mask raster in assessing dominant crop, this is fld bdry/AOI
clip_file = 'test_area.shp'

## beginning year
beg_yr = 2012

## number of years to fetch data for
end_yr = 2022

#### END USER Defined variables, edit below at your own risk ##################

## set working dir
cwd = os.getcwd()

## dirs required for structure
dirs = ['geometry', 'results', 'raster']

## CDL webservice endpoint
base_url = 'https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile'

## csv containing cdl codes, names and rgb vals
cmap_file = 'cdl_map_truncated.csv'






## BEGIN EXECUTION ############################################################
## error switch
err = False

cdl_yrs = [2009, 2024]

## check if the proposed timeframe and number of years is within CDL params
if beg_yr < cdl_yrs[0]:
    beg_yr = cdl_yrs[0]
    
if end_yr > cdl_yrs[1]:
    end_yr = cdl_yrs[1]

## check directory structure and contents
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
    cmapdf = pd.read_csv(os.path.join(cwd, cmap_file), 
                         index_col='Codes'
                         )
    
    cmapdf = cmapdf.dropna()
    ## read in geometry
    clp_shp = gpd.read_file(os.path.join(cwd, 
                                          'geometry', 
                                          clip_file)
                             )

    ## Coords must be 5070, Albers Equal Area Conic
    clp_shp = clp_shp.to_crs("EPSG:5070")

    buf = 50
    
    ## set bounding box values for the geometry
    #bb = [int(b)+i for b, i in zip(clip_shp.loc[0].geometry.bounds, [-buf,-buf,buf,buf])]
    bb = [int(b) for b in clp_shp.loc[0].geometry.bounds]
    ras = []
    
    yrs = [i for i in range(beg_yr, end_yr+1)]
    
    ctr = [(bb[1]+bb[3])/2, (bb[0]+bb[2])/2]
    ## loop over years 
    for yr in yrs:
        
        tif_str = '{0:0.0f}N-{1:0.0f}W_{2}_cdl_data.tif'
        ## download the stored data (raster for our area and this year)
        out_path = os.path.join(cwd, 
                                'raster', 
                                tif_str.format(ctr[0], ctr[1], yr)
                                )
        
        print("processing data for {0}".format(yr))
        
        ## format bounding box string for api params
        bb_str = '{0},{1},{2},{3}'.format(*bb)
        if not os.path.exists(out_path):
            ## dict for api params
            params = {'year':yr, 'bbox':bb_str}
            
            ## tell api to clip some data for our bounding box and store it on the server
            dat = req.get('https://nassgeodata.gmu.edu/axis2/services/CDLService/GetCDLFile', 
                          params
                          )
            
            ## get the url of the stored data
            root = et.fromstring(dat.content)
            dl_url = root.findtext(".//returnURL")
            
            with rio.open(dl_url,'r') as src:
                data = src.read()
                prof = src.profile
                with rio.open(out_path, 'w', **prof) as dest:    
                    dest.write(data)

        clp_ras = 0    
        ## reopen it for reading and analysis
        with rio.open(out_path, 'r') as dat:
            ras.append(dat.read(1))
            if clp_ras == 0:
                xfrm = dat.transform

            
    pdf_str = "{0:0.0f}N-{1:0.0f}W_{2}-{3}_cdl_data.pdf"
    
    out_path = os.path.join(cwd, 'results', pdf_str.format(ctr[0], 
                                                           ctr[1], 
                                                           yrs[0], 
                                                           yrs[-1]
                                                           )
                            )
    
    if not os.path.exists(out_path):
    
        ## now we need to make figs
        nrows, ncols = calc_fig_panels(len(yrs))
        
        npgs = int(len(ras)/(nrows*ncols))+1
        
        with PdfPages(out_path) as pdf:
            pg = 0
            while pg < npgs:
                
                fig, axs = plt.subplots(nrows, 
                                        ncols, 
                                        figsize=(8.5, 11),
                                        #layout="constrained"
                                        )
                
                if ncols*nrows == 1:
                    axs = [axs]
                else:
                    axs = axs.flatten()
                
                for i in range(nrows*ncols):
                    if pg*nrows*ncols+i < len(ras):
                        dat = ras[pg*nrows*ncols+i]
                        
                        ## CDL resolution changes by year so need to update msk
                        clp_ras = rasterize(clp_shp['geometry'],
                                            out_shape=dat.shape,
                                            transform=xfrm,
                                            fill=0,
                                            all_touched=True
                                            )
                        
                        vals, counts = np.unique(dat[clp_ras>0], return_counts=True)
                        crp_cod = vals[np.argmax(counts)]
                        
                        
                        crp_nam = cmapdf.loc[crp_cod, "Current Class Names"]
                        cmap, norm, tiks, labs = make_colormap(np.unique(dat), 
                                                               cmapdf
                                                               )
                        im = axs[i].imshow(dat, 
                                           extent=[bb[0], bb[2], bb[1], bb[3]], 
                                           origin='lower', 
                                           cmap=cmap, 
                                           norm=norm
                                           )
                        
                        axs[i].set_aspect('equal')
                        
                        axs[i].set_title("{0}: {1}".format(yrs[pg*nrows*ncols+i], crp_nam))
                        
                        axs[i] = clp_shp.plot(ax=axs[i],
                                              facecolor='none',
                                              edgecolor='red',
                                              linewidth=2
                                              )
                        #axs[i].ticklabel_format(style='sci', axis='both', scilimits=(0,0)) 
                        axs[i].yaxis.set_major_formatter('{x:.0f}')
                        axs[i].xaxis.set_major_formatter('{x:.0f}')
                        axs[i].tick_params(axis='y', labelsize=4)
                        axs[i].tick_params(axis='x', rotation=90, labelsize=4)
                        
                        divider = make_axes_locatable(axs[i])
                        
                        # Append on the right, 5% width, with 0.1 inch padding
                        cax = divider.append_axes("right", 
                                                  size="5%", 
                                                  pad=0.1
                                                  )
                        cb = fig.colorbar(im, 
                                          ax=axs[i], 
                                          ticks=tiks, 
                                          cax=cax
                                          )
                        
                        cb.set_ticklabels(labs, 
                                          fontsize=5
                                          )
                        
                        #cax.tick_params(labelrotation=45)
                    else:
                        axs[i].axis('off')

                pg+=1
                
                tit_str = "CDL Data for {0:.0f}N, {1:.0f}W for {2}-{3}"
                
                fig.suptitle(tit_str.format(ctr[0], 
                                            ctr[1], 
                                            yrs[0], 
                                            yrs[-1]
                                            )
                             )
                
                fig.text(0.5,
                         0.02, 
                         f"Page {pg}", 
                         ha='center', 
                         fontsize=14, 
                         color='gray'
                         )
                
                fig.tight_layout(rect=[0.05, 0.05, 0.95, 0.95])
                
                pdf.savefig(fig)
                
                plt.close(fig)
                