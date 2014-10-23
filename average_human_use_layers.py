import os
import osr
import time
import numpy
import functools

import gdal


base_table_uri = "C:/Users/rich/Desktop/average_layers_projected/all_grid_results_100km_clean.csv"

def average_layers():

    base_table_file = open(base_table_uri, 'rU')
    table_header = base_table_file.readline()

    header_extend = [
        'Lighted area density', 'Fire densities', 'FAO_Cattle', 'FAO_Goat',
        'FAO_Pig', 'FAO_Sheep', 'Human population density AG',
        'Human population density G']

    print table_header + ',' + ','.join(header_extend)

    average_raster_list = [
        ("C:/Users/rich/Desktop/average_layers_projected/lighted_area_luminosity.tif", 'Lighted area density'),
        ("C:/Users/rich/Desktop/average_layers_projected/fi_average.tif", 'Fire densities'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbctd1t0503m.tif", 'FAO_Cattle'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbgtd1t0503m.tif", 'FAO_Goat'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbpgd1t0503m.tif", 'FAO_Pig'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbshd1t0503m.tif", 'FAO_Sheep'),
        ("C:/Users/rich/Desktop/average_layers_projected/glds00ag.tif", 'Human population density AG'),
        ("C:/Users/rich/Desktop/average_layers_projected/glds00g.tif", 'Human population density G'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_11.tif', '11: Urban, Dense settlement'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_12.tif', '12: Dense settlements, Dense settlements'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_22.tif', '22: Irrigated villages, Villages'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_23.tif', '23: Cropped & pastoral villages,    Villages'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_24.tif', '24: Pastoral villages, Villages'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_25.tif', '25: Rainfed villages, Villages'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_26.tif', '26: Rainfed mosaic villages, Villages'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_31.tif', '31: Residential irrigated cropland, Croplands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_32.tif', '32: Residential rainfed mosaic, Croplands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_33.tif', '33: Populated irrigated cropland,   Croplands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_34.tif', '34: Populated rainfed cropland, Croplands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_35.tif', '35: Remote croplands, Croplands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_41.tif', '41: Residential rangelands, Rangelands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_42.tif', '42: Populated rangelands, Rangelands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_43.tif', '43: Remote rangelands, Rangelands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_51.tif', '51: Populated forests, Forested'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_52.tif', '52: Remote forests, Forested'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_61.tif', '61: Wild forests, Wildlands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_62.tif', '62: Sparse trees, Wildlands'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_63.tif', '63: Barren, Wildlands'),
        ]

    dataset_list = [gdal.Open(uri) for uri in average_raster_list]
    band_list = [ds.GetRasterBand(1) for ds in dataset_list]
    nodata_list = [band.GetNoDataValue() for band in band_list]

    for raster_uri, label in average_raster_list:
        pass

if __name__ == '__main__':
    average_layers()
