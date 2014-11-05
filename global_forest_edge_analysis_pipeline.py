import os
import shutil
import math
import time

import dill as pickle
import gdal
import osr
import numpy
import luigi

from invest_natcap import raster_utils

DATA_DIR = "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon_data"
OUTPUT_DIR = "C:/Users/rpsharp/Documents/carbon_edge_pipeline_workspace"
UNION_BIOMASS_URI = os.path.join(OUTPUT_DIR, "union_biomass.tif")
UNION_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "union_landcover.tif")
GLOBAL_BIOMASS_URI = os.path.join(OUTPUT_DIR, "intersect_biomass.tif")
GLOBAL_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "intersect_landcover.tif")
FOREST_EDGE_DISTANCE_URI = os.path.join(OUTPUT_DIR, "forest_edge_distance.tif")
ECOREGION_DATASET_URI = os.path.join(OUTPUT_DIR, "ecoregion_id.tif")
ECOREGION_SHAPEFILE_URI = os.path.join(
    DATA_DIR, 'ecoregions', 'ecoregions_projected.shp')
PREFIX_LIST = ['af', 'am', 'as']

BIOMASS_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s_biov2ct1.tif' % prefix)
    for prefix in PREFIX_LIST]
LANDCOVER_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s.tif' % prefix) for prefix in PREFIX_LIST]


class VectorizeDatasetsTask(luigi.Task):
    dataset_uri_list = luigi.Parameter(is_list=True)
    dataset_pixel_op = luigi.Parameter()
    dataset_out_uri = luigi.Parameter()
    datatype_out = luigi.Parameter()
    nodata_out = luigi.Parameter()
    pixel_size_out = luigi.Parameter()
    bounding_box_mode = luigi.Parameter()
    resample_method_list = luigi.Parameter(default=None)
    dataset_to_align_index = luigi.Parameter(default=0)
    dataset_to_bound_index = luigi.Parameter(default=None)
    aoi_uri = luigi.Parameter(default=None)
    assert_datasets_projected = luigi.Parameter(default=True)
    process_pool = luigi.Parameter(default=None)
    vectorize_op = luigi.Parameter(default=False)
    datasets_are_pre_aligned = luigi.Parameter(default=False)
    dataset_options = luigi.Parameter(default=None)
    
    def output(self):
        return luigi.LocalTarget(self.dataset_out_uri)

    def run(self):
        raster_utils.vectorize_datasets(
            list(self.dataset_uri_list), self.dataset_pixel_op,
            self.dataset_out_uri, self.datatype_out,
            self.nodata_out, self.pixel_size_out, self.bounding_box_mode,
            dataset_to_align_index=self.dataset_to_align_index,
            vectorize_op=self.vectorize_op)

class UnionRastersTask(luigi.Task):
    dataset_uri_list = luigi.Parameter(is_list=True)
    dataset_out_uri = luigi.Parameter()

    def run(self):
        def union_op(*array_list):
            output_array = array_list[0]
            for array in array_list[1:]:
                output_array = numpy.where(
                    array != nodata, array, output_array)
            return output_array
        nodata = raster_utils.get_nodata_from_uri(self.dataset_uri_list[0])
        cell_size = raster_utils.get_cell_size_from_uri(self.dataset_uri_list[0])

        raster_utils.vectorize_datasets(
            list(self.dataset_uri_list), union_op, self.dataset_out_uri,
            gdal.GDT_Int16, nodata, cell_size, "union",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(self.dataset_out_uri)

class IntersectBiomassTask(luigi.Task):
    def requires(self):
        return [
            UnionRastersTask(BIOMASS_RASTER_LIST, UNION_BIOMASS_URI),
            UnionRastersTask(LANDCOVER_RASTER_LIST, UNION_LANDCOVER_URI),
            ]

    def run(self):
        nodata = raster_utils.get_nodata_from_uri(UNION_BIOMASS_URI)
        cell_size = raster_utils.get_cell_size_from_uri(UNION_BIOMASS_URI)
        raster_utils.vectorize_datasets(
            [UNION_BIOMASS_URI, UNION_LANDCOVER_URI], lambda x,y: x,
            GLOBAL_BIOMASS_URI,
            gdal.GDT_Int16, nodata, cell_size, "intersection",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(GLOBAL_BIOMASS_URI)


class IntersectLandcoverTask(luigi.Task):
    def requires(self):
        return [
            UnionRastersTask(BIOMASS_RASTER_LIST, UNION_BIOMASS_URI),
            UnionRastersTask(LANDCOVER_RASTER_LIST, UNION_LANDCOVER_URI),
            ]

    def run(self):
        nodata = raster_utils.get_nodata_from_uri(UNION_BIOMASS_URI)
        cell_size = raster_utils.get_cell_size_from_uri(UNION_BIOMASS_URI)
        raster_utils.vectorize_datasets(
            [UNION_LANDCOVER_URI, UNION_BIOMASS_URI], lambda x,y: x,
            GLOBAL_LANDCOVER_URI,
            gdal.GDT_Int16, nodata, cell_size, "intersection",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(GLOBAL_LANDCOVER_URI)

class RasterizeEcoregion(luigi.Task):
    def requires(self):
        return [IntersectBiomassTask(), IntersectLandcoverTask()]

    def run(self):
        ecoregion_lookup = raster_utils.extract_datasource_table_by_key(
            ECOREGION_SHAPEFILE_URI, 'ECO_ID_U')
        ecoregion_nodata = -1
        ecoregion_lookup[ecoregion_nodata] = {
            'ECO_NAME': 'UNKNOWN',
            'ECODE_NAME': 'UNKNOWN',
            'WWF_MHTNAM': 'UNKNOWN',
            }

        #create ecoregion id
        raster_utils.new_raster_from_base_uri(
            GLOBAL_LANDCOVER_URI, ECOREGION_DATASET_URI, 'GTiff',
            ecoregion_nodata, gdal.GDT_Int16)
        raster_utils.rasterize_layer_uri(
            ECOREGION_DATASET_URI, ECOREGION_SHAPEFILE_URI,
            option_list=["ATTRIBUTE=ECO_ID_U"])

    def output(self):
        return luigi.LocalTarget(ECOREGION_DATASET_URI)


class CalculateForestEdge(luigi.Task):

    def requires(self):
        return IntersectLandcoverTask()

    def run(self):
        lulc_nodata = raster_utils.get_nodata_from_uri(GLOBAL_LANDCOVER_URI)

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

        cell_size = raster_utils.get_cell_size_from_uri(GLOBAL_LANDCOVER_URI)
        raster_utils.vectorize_datasets(
            [GLOBAL_LANDCOVER_URI,], mask_nonforest, mask_uri, gdal.GDT_Byte,
            mask_nodata, cell_size, 'intersection', dataset_to_align_index=0,
            dataset_to_bound_index=None, aoi_uri=None,
            assert_datasets_projected=True, process_pool=None,
            vectorize_op=False, datasets_are_pre_aligned=True)

        raster_utils.distance_transform_edt(
            mask_uri, FOREST_EDGE_DISTANCE_URI)

    def output(self):
        return luigi.LocalTarget(FOREST_EDGE_DISTANCE_URI)

def _aggregate_results(
    forest_edge_distance_uri, biomass_uri, ecoregion_dataset_uri,
    ecoregion_lookup, biomass_stats_uri):
    cell_size = raster_utils.get_cell_size_from_uri(forest_edge_distance_uri)

    forest_edge_nodata = raster_utils.get_nodata_from_uri(
        forest_edge_distance_uri)
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

    grid_resolution_list = [100]
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
                        for global_grid_resolution in grid_resolution_list:
                            #output a grid coordinate in the form 'grid_row-grid_col'
                            grid_row = int((row_coord - gt[3]) / (global_grid_resolution*1000))
                            grid_col = int((col_coord - gt[0]) / (global_grid_resolution*1000))
                            grid_id = str(grid_row) + '-' + str(grid_col)
                            outfile.write(";%s" % grid_id)
                            if grid_id not in grid_coordinates[global_grid_resolution]:
                                #convert grid coordinates to raster row_coordinates
                                grid_row_center = (grid_row + 0.5) * (global_grid_resolution*1000) + gt[3]
                                grid_col_center = (grid_col + 0.5) * (global_grid_resolution*1000) + gt[0]
                                grid_lng_coord, grid_lat_coord, _ = coord_transform.TransformPoint(
                                    grid_col_center, grid_row_center)
                                grid_coordinates[global_grid_resolution][grid_id] = (grid_lat_coord, grid_lng_coord)
                        outfile.write('\n')
    outfile.close()
    for global_grid_resolution in grid_resolution_list:
        output_dir, base_filename = os.path.split(biomass_stats_uri)
        basename = os.path.basename(base_filename)
        grid_output_file = open(os.path.join(output_dir, basename + '_' + str(global_grid_resolution) + '.csv'), 'w')
        grid_output_file.write('grid id;lat_coord;lng_coord\n')
        open(biomass_stats_uri, 'w')
        for grid_id, (lat, lng) in grid_coordinates[global_grid_resolution].iteritems():
            grid_output_file.write('%s;%s;%s\n' % (grid_id, lat, lng))
        grid_output_file.close()

class ProcessEcoregionTask(luigi.Task):
    biomass_stats_uri = os.path.join(OUTPUT_DIR, "biomass_stats.csv")

    def requires(self):
        return [CalculateForestEdge(), RasterizeEcoregion()]

    def run(self):
        ecoregion_lookup = raster_utils.extract_datasource_table_by_key(
            ECOREGION_SHAPEFILE_URI, 'ECO_ID_U')
        ecoregion_nodata = -1
        ecoregion_lookup[ecoregion_nodata] = {
            'ECO_NAME': 'UNKNOWN',
            'ECODE_NAME': 'UNKNOWN',
            'WWF_MHTNAM': 'UNKNOWN',
            }
        _aggregate_results(
            FOREST_EDGE_DISTANCE_URI, GLOBAL_BIOMASS_URI, ECOREGION_DATASET_URI,
            ecoregion_lookup, self.biomass_stats_uri)

    def output(self):
        return luigi.LocalTarget(self.biomass_stats_uri)
    

class Runit(luigi.Task):
    def requires(self):
        return ProcessEcoregionTask()


if __name__ == '__main__':
    print '\n' * 10
    if os.path.exists(OUTPUT_DIR):
        #shutil.rmtree(OUTPUT_DIR)
        pass
    #runit = Runit()
    #scheduler = scheduler.CentralPlannerScheduler()
    #for _ in range(4):
    #    scheduler.add_worker(worker.Worker())

    raster_utils.create_directories([OUTPUT_DIR])
    luigi.run()
