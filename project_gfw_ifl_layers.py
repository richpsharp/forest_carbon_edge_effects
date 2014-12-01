#project the GFW and IFL layers to rasters
import os

import gdal

from invest_natcap import raster_utils

layer_list = [
    "C:/Users/rich/Documents/Dropbox/average_layers_projected/GFW_logging_20130223_proj.shp",
    "C:/Users/rich/Documents/Dropbox/average_layers_projected/GFW_oilpalm_20130223_proj.shp",
    "C:/Users/rich/Documents/Dropbox/average_layers_projected/GFW_woodfiber_20130402_proj.shp",
    "C:/Users/rich/Documents/Dropbox/average_layers_projected/ifl2000_last_proj.shp"
]

base_uri = "C:/Users/rich/Documents/Dropbox/average_layers_projected/giant_layer.tif"

for layer_uri in layer_list:

    basename = os.path.basename(layer_uri)
    dirname = os.path.dirname(layer_uri)
    output_uri = os.path.join(dirname, os.path.splitext(basename)[0] + '.tif')
    print layer_uri, output_uri
    raster_utils.new_raster_from_base_uri(base_uri, output_uri, 'GTiff', 255,
        gdal.GDT_Byte)
    raster_utils.rasterize_layer_uri(
        output_uri, layer_uri, burn_values=[1])
