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

#DATA_DIR = "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon_data"
#OUTPUT_DIR = "C:/Users/rpsharp/Documents/carbon_edge_pipeline_workspace"
#DATA_DIR = "E:/dropboxcopy/forest_edge_carbon_data"
#OUTPUT_DIR = "E:/carbon_edge_pipeline"
DATA_DIR = "C:/Users/rich/Documents/Dropbox/forest_edge_carbon_data"
OUTPUT_DIR = "C:/Users/rich/Documents/carbon_edge_pipeline"

UNION_BIOMASS_URI = os.path.join(OUTPUT_DIR, "union_biomass.tif")
UNION_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "union_landcover.tif")
GLOBAL_BIOMASS_URI = os.path.join(OUTPUT_DIR, "intersect_biomass.tif")
GLOBAL_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "intersect_landcover.tif")
FOREST_EDGE_DISTANCE_URI = os.path.join(OUTPUT_DIR, "forest_edge_distance.tif")
ECOREGION_DATASET_URI = os.path.join(OUTPUT_DIR, "ecoregion_id.tif")
ECOREGION_SHAPEFILE_URI = os.path.join(
    DATA_DIR, 'ecoregions', 'ecoregions_projected.shp')
BIOMASS_STATS_URI = os.path.join(OUTPUT_DIR, "biomass_stats.csv")
GRID_RESOLUTION_LIST = [100]

BIOPHYSICAL_FILENAMES = [
    "global_elevation.tiff", "global_precip.tiff", "global_soil_types.tiff", "global_water_capacity.tiff",]

BIOPHYSICAL_LAYERS = [
    os.path.join(DATA_DIR, 'biophysical_layers', uri) for uri in BIOPHYSICAL_FILENAMES]

ALIGNED_BIOPHYSICAL_LAYERS = [
    os.path.join(OUTPUT_DIR, 'aligned_' + uri) for uri in BIOPHYSICAL_FILENAMES]

PREFIX_LIST = ['af', 'am', 'as']


BIOMASS_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s_biov2ct1.tif' % prefix)
    for prefix in PREFIX_LIST]
LANDCOVER_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s.tif' % prefix) for prefix in PREFIX_LIST]

for tmp_variable in ['TMP', 'TEMP', 'TMPDIR']:
    if tmp_variable in os.environ:
        print ('Updating os.environ["%s"]=%s to %s' % (tmp_variable, os.environ[tmp_variable], OUTPUT_DIR))
    else:
        print ('Setting os.environ["%s"]=%s' % (tmp_variable, OUTPUT_DIR))


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

class AlignDatasetsTask(luigi.Task):
    def requires(self):
        return [
            UnionRastersTask(BIOMASS_RASTER_LIST, UNION_BIOMASS_URI),
            UnionRastersTask(LANDCOVER_RASTER_LIST, UNION_LANDCOVER_URI),
            ]

    def run(self):
        pass

    def output(self):
        return [
            luigi.LocalTarget(uri) for uri in [GLOBAL_BIOMASS_URI, ]
        ]

class IntersectBiomassTask(luigi.Task):
    def requires(self):
        return [
            UnionRastersTask(BIOMASS_RASTER_LIST, UNION_BIOMASS_URI),
            UnionRastersTask(LANDCOVER_RASTER_LIST, UNION_LANDCOVER_URI),
            ]

    def run(self):
        nodata = raster_utils.get_nodata_from_uri(UNION_BIOMASS_URI)
        cell_size = raster_utils.get_cell_size_from_uri(UNION_LANDCOVER_URI)
        raster_utils.vectorize_datasets(
            [UNION_LANDCOVER_URI, UNION_BIOMASS_URI], lambda x, y: y,
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
        nodata = raster_utils.get_nodata_from_uri(UNION_LANDCOVER_URI)
        cell_size = raster_utils.get_cell_size_from_uri(UNION_LANDCOVER_URI)
        raster_utils.vectorize_datasets(
            [UNION_LANDCOVER_URI, UNION_BIOMASS_URI], lambda x, y: x,
            GLOBAL_LANDCOVER_URI,
            gdal.GDT_Int16, nodata, cell_size, "intersection",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(GLOBAL_LANDCOVER_URI)


class IntersectBiophysicalLayer(luigi.Task):
    biophysical_uri = luigi.Parameter()
    def requires(self):
        return IntersectBiomassTask()

    def run(self):
        nodata = raster_utils.get_nodata_from_uri(GLOBAL_BIOMASS_URI)
        cell_size = raster_utils.get_cell_size_from_uri(GLOBAL_BIOMASS_URI)
        output_uri = os.path.join(DATA_DIR, 'aligned_' + os.path.basename(self.biophysical_uri))
        raster_utils.vectorize_datasets(
            [self.biophysical_uri, GLOBAL_BIOMASS_URI], lambda x, y: x,
            output_uri, gdal.GDT_Float32, nodata, cell_size, "dataset",
            dataset_to_bound_index=1, vectorize_op=False)

    def output(self):
        output_uri = os.path.join(DATA_DIR, 'aligned_' + os.path.basename(self.biophysical_uri))
        return luigi.LocalTarget(output_uri)


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

        mask_uri = os.path.join(OUTPUT_DIR, "forest_mask.tif")
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


class ProcessEcoregionTask(luigi.Task):
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
        cell_size = raster_utils.get_cell_size_from_uri(
            FOREST_EDGE_DISTANCE_URI)
        forest_edge_nodata = raster_utils.get_nodata_from_uri(
            FOREST_EDGE_DISTANCE_URI)
        biomass_nodata = raster_utils.get_nodata_from_uri(GLOBAL_BIOMASS_URI)
        outfile = open(BIOMASS_STATS_URI, 'w')

        ecoregion_dataset = gdal.Open(ECOREGION_DATASET_URI)
        ecoregion_band = ecoregion_dataset.GetRasterBand(1)

        biomass_ds = gdal.Open(GLOBAL_BIOMASS_URI, gdal.GA_ReadOnly)
        biomass_band = biomass_ds.GetRasterBand(1)

        forest_edge_distance_ds = gdal.Open(FOREST_EDGE_DISTANCE_URI)
        forest_edge_distance_band = forest_edge_distance_ds.GetRasterBand(1)

        n_rows, n_cols = raster_utils.get_row_col_from_uri(GLOBAL_BIOMASS_URI)

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
                            for global_grid_resolution in GRID_RESOLUTION_LIST:
                                #output a grid coordinate in the form 'grid_row-grid_col'
                                grid_row = int((gt[3] - row_coord) / (global_grid_resolution*1000))
                                grid_col = int((col_coord - gt[0]) / (global_grid_resolution*1000))
                                grid_id = str(grid_row) + '-' + str(grid_col)
                                outfile.write(";%s" % grid_id)
                            outfile.write('\n')
        outfile.close()

    def output(self):
        return luigi.LocalTarget(BIOMASS_STATS_URI)
    
class ProcessGridCellLevelStats(luigi.Task):
    grid_output_file_list = [
        os.path.join(OUTPUT_DIR, 'grid_stats_%d.csv' % resolution)
        for resolution in GRID_RESOLUTION_LIST]

    def requires(self):
        for biophysical_uri in BIOPHYSICAL_LAYERS:
            yield IntersectBiophysicalLayer(biophysical_uri)
        yield IntersectBiomassTask()

    def run(self):
        biomass_ds = gdal.Open(GLOBAL_BIOMASS_URI, gdal.GA_ReadOnly)
        n_rows, n_cols = raster_utils.get_row_col_from_uri(GLOBAL_BIOMASS_URI)

        base_srs = osr.SpatialReference(biomass_ds.GetProjection())
        lat_lng_srs = base_srs.CloneGeogCS()
        coord_transform = osr.CoordinateTransformation(
            base_srs, lat_lng_srs)
        gt = biomass_ds.GetGeoTransform()

        grid_coordinates = dict((resolution, {}) for resolution in GRID_RESOLUTION_LIST)

        dataset_list = [gdal.Open(uri) for uri in ALIGNED_BIOPHYSICAL_LAYERS]
        band_list = [ds.GetRasterBand(1) for ds in dataset_list]
        nodata_list = [band.GetNoDataValue() for band in band_list]

        for global_grid_resolution, grid_output_filename in zip(GRID_RESOLUTION_LIST, self.grid_output_file_list):
            grid_output_file = open(grid_output_filename, 'w')
            grid_output_file.write('grid id,lat_coord,lng_coord')
            for biophysical_filename in BIOPHYSICAL_FILENAMES:
                grid_output_file.write(',%s' % os.path.splitext[0])
            grid_output_file.write('\n')

            n_grid_rows = int(
                (-gt[5] * n_rows) / (global_grid_resolution * 1000))
            n_grid_cols = int(
                (gt[1] * n_cols) / (global_grid_resolution * 1000))

            grid_row_stepsize = int(n_rows / float(n_grid_rows))
            grid_col_stepsize = int(n_cols / float(n_grid_cols))

            for grid_row in xrange(n_grid_rows):
                for grid_col in xrange(n_grid_cols):
                    grid_id = '%d-%d' % (grid_row, grid_col)
                    grid_row_center = -(grid_row + 0.5) * (global_grid_resolution*1000) + gt[3]
                    grid_col_center = (grid_col + 0.5) * (global_grid_resolution*1000) + gt[0]
                    grid_lng_coord, grid_lat_coord, _ = coord_transform.TransformPoint(
                        grid_col_center, grid_row_center)
                    grid_output_file.write('%s,%s,%s' % (grid_id, grid_lat_coord, grid_lng_coord))

                    global_row = grid_row * grid_row_stepsize
                    global_col = grid_col * grid_col_stepsize

                    global_col_size = min(grid_col_stepsize, n_cols - global_col)
                    global_row_size = min(grid_row_stepsize, n_rows - global_row)

                    for band, nodata in (band_list, nodata_list):
                        array = band.ReadAsArray(
                            global_col, global_row, global_col_size, global_row_size)
                        value = numpy.average(array[array != nodata])
                        grid_output_file.write(',%f' % value)

            grid_output_file.close()

    def output(self):
        return [luigi.LocalTarget(uri) for uri in self.grid_output_file_list]

class Runit(luigi.Task):
    def requires(self):
        return [ProcessEcoregionTask(), ProcessGridCellLevelStats()]


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
