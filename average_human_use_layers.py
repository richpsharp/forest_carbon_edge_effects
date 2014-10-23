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

    #need to mask the average layers to the biomass regions

    giant_layer_uri = "C:/Users/rich/Desktop/average_layers_projected/giant_layer.tif"

    af_uri = "C:/Users/rich/Desktop/af_biov2ct1.tif"
    am_uri = "C:/Users/rich/Desktop/am_biov2ct1.tif"
    as_uri = "C:/Users/rich/Desktop/as_biov2ct1.tif"
    cell_size = raster_utils.get_cell_size_from_uri(am_uri)
    raster_utils.vectorize_datasets(
        [af_uri, am_uri, as_uri], lambda x,y,z: x+y+z, giant_layer_uri, gdal.GDT_Float32,
        -1, cell_size, 'union', vectorize_op=False)

    table_uri = "C:/Users/rich/Desktop/average_layers_projected/all_grid_results_100km_clean.csv"
    lookup_table = raster_utils.get_lookup_from_csv(table_uri, 'ID100km')

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


    grid_resolution = 100 #100km
    gt = band_list[0].GetGeoTransform()
    n_rows_grid = int(-gt[5] * n_rows / (grid_resolution * 1000.0))
    n_cols_grid = int(gt[1] * n_cols / (grid_resolution * 1000.0))

    last_time = time.time()
    for grid_row_index in xrange(n_rows_grid):
        current_time = time.time()
        if current_time - last_time > 5.0:
            print "magnitude %.1f%% complete" % (grid_row_index / float(n_rows_grid) * 100)
            last_time = current_time
        for grid_col_index in xrange(n_cols_grid):
            xoff = int(grid_col_index * (grid_resolution * 1000.0) / (gt[1]))
            yoff = int(grid_row_index * (grid_resolution * 1000.0) / (-gt[5]))
            win_xsize = int((grid_resolution * 1000.0) / (gt[1]))
            win_ysize = int((grid_resolution * 1000.0) / (gt[1]))

            biomass_block = biomass_band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)
            forest_edge_distance_block = forest_edge_distance_band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)

            valid_mask = numpy.where(
                (forest_edge_distance_block != forest_edge_distance_nodata) &
                (biomass_block != biomass_nodata))

            flat_valid_biomass = biomass_block[valid_mask]

            sorted_forest_edge = numpy.argsort(flat_valid_biomass)
            flat_biomass = flat_valid_biomass[sorted_forest_edge]

            n_elements = flat_biomass.size
            if n_elements <= 10:
                continue
            lower_biomass = numpy.average(flat_biomass[0:int(n_elements*0.1)])
            upper_biomass = numpy.average(flat_biomass[int(n_elements*0.9):n_elements])

            if lower_biomass == 0:
                continue

if __name__ == '__main__':
    average_layers()
