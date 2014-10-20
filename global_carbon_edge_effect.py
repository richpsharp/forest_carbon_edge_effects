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
DATA_DIR = os.path.join("C:\\", "Users", "rich", "Desktop")
OUTPUT_DIR = os.path.join("C:\\", "Users", "rich", "Desktop", "forest_edge_output")
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
                    col_coord = gt[0] + row_coord * gt[1]

                    local_row = global_row - global_block_row*block_row_size
                    local_col = global_col - global_block_col*block_col_size

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
                        for global_grid_resolution in [25, 50, 100, 150, 200, 300, 400, 500]:
                            #output a grid coordinate in the form 'grid_row-grid_col'
                            outfile.write(";%s" % (
                                str(int(global_grid_row/(global_grid_resolution*1000))) +
                                '-' + str(int(global_grid_col/(global_grid_resolution*1000)))))
                        outfile.write('\n')

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

    input_queue = multiprocessing.JoinableQueue()
    output_queue = multiprocessing.Queue()
    
    NUMBER_OF_PROCESSES = multiprocessing.cpu_count()

    for _ in xrange(NUMBER_OF_PROCESSES):
        multiprocessing.Process(target=worker, args=(input_queue, output_queue)).start()

    pantropic_regions = ['am', 'af', 'as']
    for PREFIX in pantropic_regions:
        input_queue.put(PREFIX)
        #cProfile.runctx('process_ecoregion(PREFIX)', locals(), globals(), 'stats')
        #p = pstats.Stats('stats')
        #p.sort_stats('time').print_stats(20)
        #p.sort_stats('cumulative').print_stats(20)
        #break

    for _ in xrange(NUMBER_OF_PROCESSES):
            input_queue.put('STOP')

    input_queue.join()

    raster_utils.email_report(
        "done with global_carbon_edge_effect.py", "3152624786@txt.att.net")
