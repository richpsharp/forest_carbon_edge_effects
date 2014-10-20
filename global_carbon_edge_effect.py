import time
import os
import cProfile
import pstats
import multiprocessing

import gdal
import osr
import numpy
import math
import sys

from invest_natcap import raster_utils

BIOMASS_PREFIXES = ['am', 'af', 'as']
DATA_DIR = os.path.join("C://", "Users", "rich", "Desktop")
OUTPUT_DIR = os.path.join("C://", "Users", "rich", "Desktop", "forest_edge_output")
BIOMASS_BASE = '_biov2ct1.tif'
LULC_BASE = '.tif'
FOREST_LULCS = [1, 2, 3, 4, 5]

#i calculated these values by loading all 3 rasters in qgis and manually writing down the bounding boxes and taking the extremes
GLOBAL_UPPER_LEFT_ROW = -2602195.7925872812047601
GLOBAL_UPPER_LEFT_COL = -11429693.3490753173828125

def lowpriority():
    """ Set the priority of the process to below-normal."""
    try:
        sys.getwindowsversion()
    except:
        is_windows = False
    else:
        is_windows = True

    if is_windows:
        # Based on:
        #   "Recipe 496767: Set Process Priority In Windows" on ActiveState
        #   http://code.activestate.com/recipes/496767/
        import win32api, win32process, win32con

        pid = win32api.GetCurrentProcessId()
        handle = win32api.OpenProcess(win32con.PROCESS_ALL_ACCESS, True, pid)
        win32process.SetPriorityClass(handle, win32process.IDLE_PRIORITY_CLASS)
    else:
        os.nice(1)

def worker(input_queue, output_queue):
    lowpriority()
    for func, args in iter(input_queue.get, 'STOP'):
        result = func(*args)
        output_queue.put(result)
        input_queue.task_done()
    input_queue.task_done()


def _make_magnitude_maps(base_uri, table_uri):
    grid_resolution = 100
    
    base_ds = gdal.Open(base_uri)
    
    projection = base_ds.GetProjection()
    driver = gdal.GetDriverByName('GTiff')

    gt = base_ds.GetGeoTransform()

    n_rows = base_ds.RasterYSize
    n_cols = base_ds.RasterXSize

    lookup_table = raster_utils.get_lookup_from_csv(table_uri, 'ID100km')
    
    for map_type in ['Magnitude', 'A80', 'A90', 'A95']:

        output_dir, base_filename = os.path.split(base_uri)
        basename = os.path.basename(base_filename)

        output_uri = os.path.join(output_dir, map_type + '.tif')

        n_rows_grid = int(-gt[5] * n_rows / (grid_resolution * 1000.0))
        n_cols_grid = int(gt[1] * n_cols / (grid_resolution * 1000.0))

        new_geotransform = (
            gt[0], grid_resolution * 1000.0, gt[2],
            gt[3], gt[4], -grid_resolution * 1000.0)

        n_rows_grid = max([int(grid_cell.split('-')[0]) for grid_cell in lookup_table])
        n_cols_grid = max([int(grid_cell.split('-')[1]) for grid_cell in lookup_table])

        n_rows_grid += 1
        n_cols_grid += 1

        output_ds = driver.Create(
            output_uri.encode('utf-8'), n_cols_grid, n_rows_grid, 1, gdal.GDT_Float32)
        output_ds.SetProjection(projection)
        output_ds.SetGeoTransform(new_geotransform)
        output_band = output_ds.GetRasterBand(1)

        output_nodata = -9999
        output_band.SetNoDataValue(output_nodata)
        output_band.Fill(output_nodata)

        last_time = time.time()
        for grid_id in lookup_table:
            current_time = time.time()
            if current_time - last_time > 5.0:
                print "%s working..." % (map_type,)
                last_time = current_time

            grid_row_index, grid_col_index = map(int, grid_id.split('-'))

            try:            
                output_band.WriteArray(
                    numpy.array([[float(lookup_table[grid_id][map_type])]]),
                    xoff=grid_col_index, yoff=n_rows_grid - grid_row_index - 1)
            except ValueError:
                pass


def _map_intensity(forest_edge_distance_uri, biomass_uri):
    grid_resolution_list = [25, 50, 100, 150, 200, 300, 400, 500]
    
    forest_edge_distance_ds = gdal.Open(forest_edge_distance_uri)
    forest_edge_distance_band = forest_edge_distance_ds.GetRasterBand(1)
    forest_edge_distance_nodata = raster_utils.get_nodata_from_uri(forest_edge_distance_uri)

    biomass_ds = gdal.Open(biomass_uri)
    biomass_band = biomass_ds.GetRasterBand(1)
    biomass_nodata = raster_utils.get_nodata_from_uri(biomass_uri)

    n_rows = biomass_ds.RasterYSize
    n_cols = biomass_ds.RasterXSize

    projection = biomass_ds.GetProjection()
    geotransform = biomass_ds.GetGeoTransform()
    driver = gdal.GetDriverByName('GTiff')

    gt = biomass_ds.GetGeoTransform()

    for grid_resolution in grid_resolution_list:

        output_dir, base_filename = os.path.split(biomass_uri)
        basename = os.path.basename(base_filename)

        output_uri = os.path.join(
            output_dir, basename + '_intensity_' + str(grid_resolution) + '.tif')

        n_rows_grid = int(-gt[5] * n_rows / (grid_resolution * 1000.0))
        n_cols_grid = int(gt[1] * n_cols / (grid_resolution * 1000.0))

        new_geotransform = (
            gt[0], grid_resolution * 1000.0, gt[2],
            gt[3], gt[4], -grid_resolution * 1000.0)

        
        output_ds = driver.Create(
            output_uri.encode('utf-8'), n_cols_grid, n_rows_grid, 1, gdal.GDT_Float32)
        output_ds.SetProjection(projection)
        output_ds.SetGeoTransform(new_geotransform)
        output_band = output_ds.GetRasterBand(1)

        output_nodata = -1
        output_band.SetNoDataValue(output_nodata)
        output_band.Fill(output_nodata)

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

                magnitude = upper_biomass/lower_biomass

                output_band.WriteArray(
                    numpy.array([[magnitude]]),
                    xoff=grid_col_index, yoff=grid_row_index)


def _aggregate_results(forest_edge_distance_uri, biomass_uri, ecoregion_dataset_uri, ecoregion_lookup, biomass_stats_uri):
    cell_size = raster_utils.get_cell_size_from_uri(forest_edge_distance_uri)

    forest_edge_nodata = raster_utils.get_nodata_from_uri(forest_edge_distance_uri)
    biomass_nodata = raster_utils.get_nodata_from_uri(biomass_uri)    

    outfile = open(biomass_stats_uri, 'w')

    ecoregion_dataset = gdal.Open(ecoregion_dataset_uri)
    ecoregion_band = ecoregion_dataset.GetRasterBand(1)

    biomass_ds = gdal.Open(biomass_uri, gdal.GA_ReadOnly)
    biomass_band = biomass_ds.GetRasterBand(1)

    forest_edge_distance_ds = gdal.Open(forest_edge_distance_uri)
    forest_edge_distance_band = forest_edge_distance_ds.GetRasterBand(1)

    n_rows, n_cols = raster_utils.get_row_col_from_uri(biomass_uri)

    base_srs = osr.SpatialReference(biomass_ds.GetProjection())
    lat_lng_srs = base_srs.CloneGeogCS()
    coord_transform = osr.CoordinateTransformation(
        base_srs, lat_lng_srs)
    gt = biomass_ds.GetGeoTransform()

    grid_resolution_list = [25, 50, 100, 150, 200, 300, 400, 500]
    grid_coordinates = dict((resolution, {}) for resolution in grid_resolution_list)

    block_col_size, block_row_size = biomass_band.GetBlockSize()
    n_global_block_rows = int(math.ceil(float(n_rows) / block_row_size))
    n_global_block_cols = int(math.ceil(float(n_cols) / block_col_size))
    
    last_time = time.time()
    for global_block_row in xrange(n_global_block_rows):
        current_time = time.time()
        if current_time - last_time > 5.0:
            print "aggregation %.1f%% complete" % (global_block_row / float(n_global_block_rows) * 100)
            last_time = current_time
        for global_block_col in xrange(n_global_block_cols):
            xoff = global_block_col * block_col_size
            yoff = global_block_row * block_row_size
            win_xsize = min(block_col_size, n_cols - xoff)
            win_ysize = min(block_row_size, n_rows - yoff)
            biomass_block = biomass_band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)
            forest_edge_distance_block = forest_edge_distance_band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)
            ecoregion_id_block = ecoregion_band.ReadAsArray(
                xoff=xoff, yoff=yoff, win_xsize=win_xsize, win_ysize=win_ysize)

            for global_row in xrange(global_block_row*block_row_size, min((global_block_row+1)*block_row_size, n_rows)):
                for global_col in xrange(global_block_col*block_col_size, min((global_block_col+1)*block_col_size, n_cols)):
                    row_coord = gt[3] + global_row * gt[5]    
                    col_coord = gt[0] + global_col * gt[1]

                    local_row = global_row - global_block_row * block_row_size
                    local_col = global_col - global_block_col * block_col_size

                    lng_coord, lat_coord, _ = coord_transform.TransformPoint(
                        col_coord, row_coord)

                    #normalize the coordinates so they don't go negative
                    global_grid_row = row_coord - GLOBAL_UPPER_LEFT_ROW
                    global_grid_col = col_coord - GLOBAL_UPPER_LEFT_COL

                    ecoregion_id = ecoregion_id_block[local_row, local_col]
                    if (forest_edge_distance_block[local_row, local_col] != forest_edge_nodata and
                            forest_edge_distance_block[local_row, local_col] > 0.0 and
                            biomass_block[local_row, local_col] != biomass_nodata):

                        outfile.write("%f;%f;%f;%f;%s;%s;%s" % (
                            forest_edge_distance_block[local_row, local_col] * cell_size,
                            biomass_block[local_row, local_col], lat_coord, lng_coord,
                            ecoregion_lookup[ecoregion_id]['ECO_NAME'],
                            ecoregion_lookup[ecoregion_id]['ECODE_NAME'],
                            ecoregion_lookup[ecoregion_id]['WWF_MHTNAM']))
                        outfile.write(";%f;%f" % (global_grid_row, global_grid_col))
                        for global_grid_resolution in grid_resolution_list:
                            #output a grid coordinate in the form 'grid_row-grid_col'
                            grid_row = int(global_grid_row/(global_grid_resolution*1000))
                            grid_col = int(global_grid_col/(global_grid_resolution*1000))
                            grid_id = str(grid_row) + '-' + str(grid_col)
                            outfile.write(";%s" % grid_id)
                            if grid_id not in grid_coordinates[global_grid_resolution]:
                                grid_row_center = grid_row * global_grid_resolution*1000 + GLOBAL_UPPER_LEFT_ROW
                                grid_col_center = grid_col * global_grid_resolution*1000 + GLOBAL_UPPER_LEFT_COL
                                grid_lng_coord, grid_lat_coord, _ = coord_transform.TransformPoint(
                                    grid_col_center, grid_row_center)
                                grid_coordinates[global_grid_resolution][grid_id] = (grid_lat_coord, grid_lng_coord)
                                print grid_lat_coord, grid_lng_coord
                        outfile.write('/n')
    outfile.close()
    for global_grid_resolution in grid_resolution_list:
        output_dir, base_filename = os.path.split(biomass_stats_uri)
        basename = os.path.basename(base_filename)
        grid_output_file = open(os.path.join(output_dir, basename + '_' + str(global_grid_resolution) + '.csv'), 'w')
        grid_output_file.write('grid id;lat_coord;lng_coord/n')
        open(biomass_stats_uri, 'w')
        for grid_id, (lat, lng) in grid_coordinates[global_grid_resolution].iteritems():
            grid_output_file.write('%s;%s;%s/n' % (grid_id, lat, lng))
        grid_output_file.close()

def process_ecoregion(prefix):
    ecoregion_shapefile_uri = os.path.join(
        DATA_DIR, 'ecoregions', 'ecoregions_projected.shp')

    ecoregion_lookup = raster_utils.extract_datasource_table_by_key(
        ecoregion_shapefile_uri, 'ECO_ID_U')
    ecoregion_nodata = -1
    ecoregion_lookup[ecoregion_nodata] = {
        'ECO_NAME': 'UNKNOWN',
        'ECODE_NAME': 'UNKNOWN',
        'WWF_MHTNAM': 'UNKNOWN',
        }

    lulc_raw_uri = os.path.join(DATA_DIR, '%s%s' % (prefix, LULC_BASE))
    biomass_raw_uri = os.path.join(DATA_DIR, '%s%s' % (prefix, BIOMASS_BASE))

    cell_size = raster_utils.get_cell_size_from_uri(lulc_raw_uri)

    lulc_uri = os.path.join(OUTPUT_DIR, "%s_lulc_aligned.tif" % (prefix))
    biomass_uri = os.path.join(OUTPUT_DIR, "%s_biomass_aligned.tif" % (prefix))

    raster_utils.align_dataset_list(
        [lulc_raw_uri, biomass_raw_uri], [lulc_uri, biomass_uri], ['nearest']*2,
        cell_size, 'intersection', 0, dataset_to_bound_index=None,
        aoi_uri=None, assert_datasets_projected=True, process_pool=None)

    #create ecoregion id
    ecoregion_dataset_uri = os.path.join(
        OUTPUT_DIR, "%s_ecoregion_id.tif" % (prefix))
    raster_utils.new_raster_from_base_uri(
        lulc_uri, ecoregion_dataset_uri, 'GTiff', ecoregion_nodata, gdal.GDT_Int16)
    raster_utils.rasterize_layer_uri(
        ecoregion_dataset_uri, ecoregion_shapefile_uri,
        option_list=["ATTRIBUTE=ECO_ID_U"])

    lulc_nodata = raster_utils.get_nodata_from_uri(lulc_uri)

    forest_lulc_codes = [1, 2, 3, 4, 5]

    mask_uri = os.path.join(OUTPUT_DIR, "%s_mask.tif" % prefix)
    mask_nodata = 2

    def mask_nonforest(lulc):
        mask = numpy.empty(lulc.shape, dtype=numpy.int8)
        mask[:] = 1
        for lulc_code in forest_lulc_codes:
            mask[lulc == lulc_code] = 0
        mask[lulc == lulc_nodata] = mask_nodata
        return mask

    raster_utils.vectorize_datasets(
        [lulc_uri,], mask_nonforest, mask_uri, gdal.GDT_Byte,
        mask_nodata, cell_size, 'intersection', dataset_to_align_index=0,
        dataset_to_bound_index=None, aoi_uri=None,
        assert_datasets_projected=True, process_pool=None, vectorize_op=False,
        datasets_are_pre_aligned=True)

    forest_edge_distance_uri = os.path.join(OUTPUT_DIR, "%s_forest_edge.tif" % prefix)
    raster_utils.distance_transform_edt(mask_uri, forest_edge_distance_uri)

    biomass_stats_uri = os.path.join(OUTPUT_DIR, "%s_biomass_stats.csv" % prefix)
    _aggregate_results(forest_edge_distance_uri, biomass_uri, ecoregion_dataset_uri, ecoregion_lookup, biomass_stats_uri)
    

if __name__ == '__main__':
    #mask out forest LULCs
    raster_utils.create_directories([OUTPUT_DIR])

    INPUT_QUEUE = multiprocessing.JoinableQueue()
    OUTPUT_QUEUE = multiprocessing.Queue()
    
    NUMBER_OF_PROCESSES = multiprocessing.cpu_count()

    for _ in xrange(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(INPUT_QUEUE, OUTPUT_QUEUE)).start()

    pantropic_regions = ['am', 'af', 'as']
    for PREFIX in pantropic_regions:
        pass
        #INPUT_QUEUE.put((process_ecoregion, [PREFIX,]))
        #cProfile.runctx('process_ecoregion(PREFIX)', locals(), globals(), 'stats')
        #p = pstats.Stats('stats')
        #p.sort_stats('time').print_stats(20)
        #p.sort_stats('cumulative').print_stats(20)
        #break

    for _ in xrange(NUMBER_OF_PROCESSES):
        INPUT_QUEUE.put('STOP')
    INPUT_QUEUE.join()
        
    for PREFIX in pantropic_regions:
        pass
        #INPUT_QUEUE.put((_map_intensity, ["C:/Users/rich/Desktop/forest_edge_output/%s_forest_edge.tif" % PREFIX,
        #    "C:/Users/rich/Desktop/forest_edge_output/%s_biomass_aligned.tif" % PREFIX]))
        #_map_intensity(
        #    "C:/Users/rich/Desktop/forest_edge_output/%s_forest_edge.tif" % PREFIX,
        #    "C:/Users/rich/Desktop/forest_edge_output/%s_biomass_aligned.tif" % PREFIX)

    BASE_URI = "C:/Users/rich/Desktop/forest_edge_output/af_biomass_aligned.tif"
    TABLE_URI = "C:/Users/rich/Desktop/forest_edge_output/all_grid_results_100km_clean.csv"
    _make_magnitude_maps(BASE_URI, TABLE_URI)



    raster_utils.email_report(
        "done with global_carbon_edge_effect.py", "3152624786@txt.att.net")
