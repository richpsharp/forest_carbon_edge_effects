import os
import time
import numpy
import functools
import sys
import codecs
import types

import gdal
import osr

from invest_natcap import raster_utils

GLOBAL_UPPER_LEFT_ROW = -2602195.7925872812047601
GLOBAL_UPPER_LEFT_COL = -11429693.3490753173828125


def average_layers():

    base_table_uri = "C:/Users/rich/Desktop/all_grid_results_100km_clean_v2.csv"
    base_table_file = open(base_table_uri, 'rU')
    table_header = base_table_file.readline()

    #need to mask the average layers to the biomass regions

    giant_layer_uri = "C:/Users/rich/Desktop/average_layers_projected/giant_layer.tif"

    af_uri = "C:/Users/rich/Desktop/af_biov2ct1.tif"
    am_uri = "C:/Users/rich/Desktop/am_biov2ct1.tif"
    as_uri = "C:/Users/rich/Desktop/as_biov2ct1.tif"
    cell_size = raster_utils.get_cell_size_from_uri(am_uri)
    #raster_utils.vectorize_datasets(
    #    [af_uri, am_uri, as_uri], lambda x,y,z: x+y+z, giant_layer_uri, gdal.GDT_Float32,
    #    -1, cell_size, 'union', vectorize_op=False)

    table_uri = base_table_uri
    table_file = open(table_uri, 'rU')
    
    table_header = table_file.readline().rstrip()


    lookup_table = raster_utils.get_lookup_from_csv(table_uri, 'ID100km')

    out_table_uri =  "C:/Users/rich/Desktop/all_grid_results_100km_human.csv"
    out_table_file = codecs.open(out_table_uri, 'w', 'utf-8')

    average_raster_list = [
        ("C:/Users/rich/Desktop/average_layers_projected/lighted_area_luminosity.tif", 'Lighted area density'),
        ("C:/Users/rich/Desktop/average_layers_projected/fi_average.tif", 'Fire densities'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbctd1t0503m.tif", 'FAO_Cattle'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbgtd1t0503m.tif", 'FAO_Goat'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbpgd1t0503m.tif", 'FAO_Pig'),
        ("C:/Users/rich/Desktop/average_layers_projected/glbshd1t0503m.tif", 'FAO_Sheep'),
        ("C:/Users/rich/Desktop/average_layers_projected/glds00ag.tif", 'Human population density AG'),
        ("C:/Users/rich/Desktop/average_layers_projected/glds00g.tif", 'Human population density G'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_11.tif', '"11: Urban, Dense settlement"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_12.tif', '"12: Dense settlements, Dense settlements"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_22.tif', '"22: Irrigated villages, Villages"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_23.tif', '"23: Cropped & pastoral villages, Villages"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_24.tif', '"24: Pastoral villages, Villages"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_25.tif', '"25: Rainfed villages, Villages"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_26.tif', '"26: Rainfed mosaic villages, Villages"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_31.tif', '"31: Residential irrigated cropland, Croplands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_32.tif', '"32: Residential rainfed mosaic, Croplands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_33.tif', '"33: Populated irrigated cropland,   Croplands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_34.tif', '"34: Populated rainfed cropland, Croplands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_35.tif', '"35: Remote croplands, Croplands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_41.tif', '"41: Residential rangelands, Rangelands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_42.tif', '"42: Populated rangelands, Rangelands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_43.tif', '"43: Remote rangelands, Rangelands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_51.tif', '"51: Populated forests, Forested"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_52.tif', '"52: Remote forests, Forested"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_61.tif', '"61: Wild forests, Wildlands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_62.tif', '"62: Sparse trees, Wildlands"'),
        ('C:/Users/rich/Desktop/average_layers_projected/anthrome_63.tif', '"63: Barren, Wildlands"'),
        ]

    clipped_raster_list = []


    for average_raster_uri, header in average_raster_list:
        print 'clipping ' + average_raster_uri
        clipped_raster_uri = os.path.join(os.path.dirname(average_raster_uri), 'temp', os.path.basename(average_raster_uri))
        cell_size = raster_utils.get_cell_size_from_uri(average_raster_uri)
        #raster_utils.vectorize_datasets(
        #    [average_raster_uri, giant_layer_uri], lambda x,y: x, clipped_raster_uri, gdal.GDT_Float32,
        #    -1, cell_size, 'intersection', vectorize_op=False)
        clipped_raster_list.append((clipped_raster_uri, header))

    dataset_list = [gdal.Open(uri) for uri, label in clipped_raster_list]
    band_list = [ds.GetRasterBand(1) for ds in dataset_list]
    nodata_list = [band.GetNoDataValue() for band in band_list]

    extended_table_headers = ','.join([header for _, header in average_raster_list])


    def write_to_file(value):
        try:
            out_table_file.write(value)
        except UnicodeDecodeError as e:
            out_table_file.write(value.decode('latin-1'))

    write_to_file(table_header + ',' + extended_table_headers + '\n')
    #print table_header + ',' + extended_table_headers

    for line in table_file:
        split_line = line.rstrip().split(',')
        grid_id = split_line[2]
    #for grid_id in lookup_table:
        try:
            split_grid_id = grid_id.split('-')
            grid_row_index, grid_col_index = map(int, split_grid_id)
        except ValueError as e:
            month_to_number = {
                'Jan': 1,
                'Feb': 2,
                'Mar': 3,
                'Apr': 4,
                'May': 5,
                'Jun': 6,
                'Jul': 7,
                'Aug': 8,
                'Sep': 9,
                'Oct': 10,
                'Nov': 11,
                'Dec': 12,
            }
            grid_row_index, grid_col_index = month_to_number[split_grid_id[0]], int(split_grid_id[1])
            
        print 'processing grid id ' + grid_id

        ds = dataset_list[0]
        base_srs = osr.SpatialReference(ds.GetProjection())
        lat_lng_srs = base_srs.CloneGeogCS()
        coord_transform = osr.CoordinateTransformation(
            base_srs, lat_lng_srs)
        gt = ds.GetGeoTransform()
        grid_resolution = 100 #100km
        
        row_coord = grid_row_index * grid_resolution * 1000 + GLOBAL_UPPER_LEFT_ROW
        col_coord = grid_col_index * grid_resolution * 1000 + GLOBAL_UPPER_LEFT_COL

        lng_coord, lat_coord, _ = coord_transform.TransformPoint(
            col_coord, row_coord)
        write_to_file(','.join(split_line[0:2]) + ',%d-%d,' % (grid_row_index, grid_col_index) + ','.join(split_line[3:11]) +',%f,%f,' % (lat_coord, lng_coord)+','.join(split_line[13:]))

#        print ','.join(split_line[0:11]), ',%f,%f,' % (lat_coord, lng_coord), ','.join(split_line[14:19]),

        for (_, header), band, ds, nodata in zip(clipped_raster_list, band_list, dataset_list, nodata_list):

            gt = ds.GetGeoTransform()
            n_rows = ds.RasterYSize
            n_cols = ds.RasterXSize
               
            xoff = int(grid_col_index * (grid_resolution * 1000.0) / (gt[1]))
            yoff = int(grid_row_index * (grid_resolution * 1000.0) / (-gt[5]))
            win_xsize = int((grid_resolution * 1000.0) / (gt[1]))
            win_ysize = int((grid_resolution * 1000.0) / (gt[1]))

            if xoff + win_xsize > n_cols:
                win_xsize = n_cols - xoff
            if yoff + win_ysize > n_rows:
                win_ysize = n_rows - yoff

            block = band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)
            block_average = numpy.average(block[block != nodata])
            #sys.stdout.write(',%f' % block_average)
            write_to_file(',%f' % block_average)
            #print block_average, header, grid_row_index, grid_col_index
        write_to_file('\n')

if __name__ == '__main__':
    average_layers()
