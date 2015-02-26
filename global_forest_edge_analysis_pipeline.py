"""This module is used to process the data stack for analysis of forest carbon
    edges, their relationship to biomass stocks and other biophysical and
    anthromic data."""

import os
import math
import time
import glob

import dill as pickle
import gdal
import ogr
import osr
import numpy
import scipy.stats
import luigi

from invest_natcap import raster_utils


BASE_DATA_DIR = "C:/Users/rpsharp/Dropbox_stanford/Dropbox/"
DATA_DIR = os.path.join(BASE_DATA_DIR, "forest_edge_carbon_data")
AVERAGE_LAYERS_DIRECTORY = os.path.join(
    BASE_DATA_DIR, "average_layers_projected")

OUTPUT_DIR = "C:/Users/rpsharp/Desktop/carbon_edge_pipeline_workspace"
UNION_BIOMASS_URI = os.path.join(OUTPUT_DIR, "union_biomass.tif")
UNION_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "union_landcover.tif")
GLOBAL_BIOMASS_URI = os.path.join(OUTPUT_DIR, "intersect_biomass.tif")
GLOBAL_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "intersect_landcover.tif")
FOREST_EDGE_DISTANCE_URI = os.path.join(OUTPUT_DIR, "forest_edge_distance.tif")
ECOREGION_DATASET_URI = os.path.join(OUTPUT_DIR, "ecoregion_id.tif")
ECOREGION_SHAPEFILE_URI = os.path.join(
    DATA_DIR, 'ecoregions', 'ecoregions_projected.shp')
BIOMASS_STATS_URI = os.path.join(OUTPUT_DIR, "biomass_stats.csv")

#This is a 12 band raster of monthly precip
GLOBAL_PRECIP_URI = os.path.join(
    DATA_DIR, "biophysical_layers", "global_precip.tiff")

#It will be summed into this
TOTAL_PRECIP_URI = os.path.join(OUTPUT_DIR, 'total_precip.tif')
ALIGNED_TOTAL_PRECIP_URI = os.path.join(
    OUTPUT_DIR, 'aligned_' + os.path.basename(TOTAL_PRECIP_URI))

#Dry season length will be calcualted from total precip
DRY_SEASON_LENGTH_URI = os.path.join(OUTPUT_DIR, 'dry_season_length.tif')
ALIGNED_DRY_SEASON_LENGTH_URI = os.path.join(
    OUTPUT_DIR, 'aligned_' + os.path.basename(DRY_SEASON_LENGTH_URI))

GRID_RESOLUTION_LIST = [100]

#I got these on the online ORNL site
BIOPHYSICAL_FILENAMES = [
    "global_elevation.tiff", "global_water_capacity.tiff",]

#This is off the ORNL site too but must be processed differently
GLOBAL_SOIL_TYPES_URI = os.path.join(
    "biophysical_layers", "global_soil_types.tiff")
LAYERS_TO_MAX = [os.path.join(DATA_DIR, GLOBAL_SOIL_TYPES_URI)]

#these are the biophysical layers i downloaded from the ornl website
LAYERS_TO_AVERAGE = [
    os.path.join(DATA_DIR, 'biophysical_layers', URI)
    for URI in BIOPHYSICAL_FILENAMES]
#these are the human use layers becky sent me once
LAYERS_TO_AVERAGE += glob.glob(os.path.join(AVERAGE_LAYERS_DIRECTORY, '*.tif'))

ALIGNED_LAYERS_TO_AVERAGE = [
    os.path.join(OUTPUT_DIR, 'aligned_' + os.path.basename(URI))
    for URI in LAYERS_TO_AVERAGE]
ALIGNED_LAYERS_TO_AVERAGE.append(ALIGNED_TOTAL_PRECIP_URI)
ALIGNED_LAYERS_TO_AVERAGE.append(ALIGNED_DRY_SEASON_LENGTH_URI)

ALIGNED_LAYERS_TO_MAX = [
    os.path.join(
        OUTPUT_DIR, 'aligned_' + os.path.basename(URI))
    for URI in LAYERS_TO_MAX]

PREFIX_LIST = ['af', 'am', 'as']
BIOMASS_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s_biov2ct1.tif' % prefix)
    for prefix in PREFIX_LIST]
LANDCOVER_RASTER_LIST = [
    os.path.join(DATA_DIR, '%s.tif' % prefix) for prefix in PREFIX_LIST]

for tmp_variable in ['TMP', 'TEMP', 'TMPDIR']:
    if tmp_variable in os.environ:
        print (
            'Updating os.environ["%s"]=%s to %s' %
            (tmp_variable, os.environ[tmp_variable], OUTPUT_DIR))
    else:
        print 'Setting os.environ["%s"]=%s' % (tmp_variable, OUTPUT_DIR)
    os.environ[tmp_variable] = OUTPUT_DIR

class VectorizeDatasetsTask(luigi.Task):
    """Stages up all the datasets to vectorize"""

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
    """LUIGI task to union rasters together that may not overlap in space
        the result is a single output raster with the bounding box extents
        covering all the rasters and data defined in the stack that is defined
        in at least one input raster.  We use it in this context to join
        the contenential rasters togeter into a global one"""

    dataset_uri_list = luigi.Parameter(is_list=True)
    dataset_out_uri = luigi.Parameter()

    def run(self):
        def union_op(*array_list):
            """Given an array stack return an array that has a value defined
                in the stack that is not nodata.  used for overlapping nodata
                stacks."""
            output_array = array_list[0]
            for array in array_list[1:]:
                output_array = numpy.where(
                    array != nodata, array, output_array)
            return output_array
        nodata = raster_utils.get_nodata_from_uri(self.dataset_uri_list[0])
        cell_size = raster_utils.get_cell_size_from_uri(
            self.dataset_uri_list[0])

        raster_utils.vectorize_datasets(
            list(self.dataset_uri_list), union_op, self.dataset_out_uri,
            gdal.GDT_Int16, nodata, cell_size, "union",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(self.dataset_out_uri)


class IntersectBiomassTask(luigi.Task):
    """Luigi task to align the separate biomass, intersect the
        result into two prefectly aligned global rasters"""
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
    """Luigi task to align the separate landcover maps and intersect the
        result into two prefectly aligned global rasters"""
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


def _align_raster_with_biomass(input_uri, output_uri):
    """Function to use internally to take an input and align it with the
        GLOBAL_BIOMASS_URI raster"""
    nodata = raster_utils.get_nodata_from_uri(input_uri)
    if nodata is None:
        nodata = -9999
    cell_size = raster_utils.get_cell_size_from_uri(GLOBAL_BIOMASS_URI)
    raster_utils.vectorize_datasets(
        [input_uri, GLOBAL_BIOMASS_URI], lambda x, y: x,
        output_uri, gdal.GDT_Float32, nodata, cell_size, "dataset",
        dataset_to_bound_index=1, vectorize_op=False)


class AlignLayerWithBiomassTask(luigi.Task):
    """Task to trigger an alignment with the input parameter to the biomass
        raster"""
    input_uri = luigi.Parameter()
    def requires(self):
        return IntersectBiomassTask()

    def run(self):
        output_uri = os.path.join(
            OUTPUT_DIR, 'aligned_' + os.path.basename(self.input_uri))
        _align_raster_with_biomass(self.input_uri, output_uri)

    def output(self):
        output_uri = os.path.join(
            OUTPUT_DIR, 'aligned_' + os.path.basename(self.input_uri))
        return luigi.LocalTarget(output_uri)


class RasterizeEcoregion(luigi.Task):
    """Luigi task to rasterize the shapefile ecoregion to a raster that
        aligns with the global landcover raster"""
    def requires(self):
        return [IntersectLandcoverTask()]

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
    """Luigi task to create a forest mask from the landcover raster"""
    def requires(self):
        return IntersectLandcoverTask()

    def run(self):
        lulc_nodata = raster_utils.get_nodata_from_uri(GLOBAL_LANDCOVER_URI)

        forest_lulc_codes = [1, 2, 3, 4, 5]

        mask_uri = os.path.join(OUTPUT_DIR, "forest_mask.tif")
        mask_nodata = 2

        def mask_nonforest(lulc):
            """Takes in a numpy array of landcover values and returns 1s
                where they match forest codes and 0 otherwise"""
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
    """A luigi task to aggregate forest edge pixel by pixel and indicate
        the grid coordinate the pixel lies in and the ecoregion in which
        it does."""
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
        geo_trans = biomass_ds.GetGeoTransform()

        block_col_size, block_row_size = biomass_band.GetBlockSize()
        n_global_block_rows = int(math.ceil(float(n_rows) / block_row_size))
        n_global_block_cols = int(math.ceil(float(n_cols) / block_col_size))

        last_time = time.time()
        for global_block_row in xrange(n_global_block_rows):
            current_time = time.time()
            if current_time - last_time > 5.0:
                print (
                    "aggregation %.1f%% complete" %
                    (global_block_row / float(n_global_block_rows) * 100))
                last_time = current_time
            for global_block_col in xrange(n_global_block_cols):
                xoff = global_block_col * block_col_size
                yoff = global_block_row * block_row_size
                win_xsize = min(block_col_size, n_cols - xoff)
                win_ysize = min(block_row_size, n_rows - yoff)
                biomass_block = biomass_band.ReadAsArray(
                    xoff=xoff, yoff=yoff, win_xsize=win_xsize,
                    win_ysize=win_ysize)
                forest_edge_distance_block = (
                    forest_edge_distance_band.ReadAsArray(
                        xoff=xoff, yoff=yoff, win_xsize=win_xsize,
                        win_ysize=win_ysize))
                ecoregion_id_block = ecoregion_band.ReadAsArray(
                    xoff=xoff, yoff=yoff, win_xsize=win_xsize,
                    win_ysize=win_ysize)

                for global_row in xrange(
                        global_block_row*block_row_size,
                        min((global_block_row+1)*block_row_size, n_rows)):
                    for global_col in xrange(
                            global_block_col*block_col_size,
                            min((global_block_col+1)*block_col_size, n_cols)):
                        row_coord = (
                            geo_trans[3] + global_row * geo_trans[5])
                        col_coord = (
                            geo_trans[0] + global_col * geo_trans[1])

                        local_row = (
                            global_row - global_block_row * block_row_size)
                        local_col = (
                            global_col - global_block_col * block_col_size)

                        lng_coord, lat_coord, _ = (
                            coord_transform.TransformPoint(
                                col_coord, row_coord))

                        ecoregion_id = ecoregion_id_block[local_row, local_col]
                        if (forest_edge_distance_block[local_row, local_col] !=
                                forest_edge_nodata and
                                forest_edge_distance_block
                                [local_row, local_col] > 0.0 and
                                biomass_block
                                [local_row, local_col] != biomass_nodata):
                            outfile.write("%f;%f;%f;%f;%s;%s;%s" % (
                                forest_edge_distance_block
                                [local_row, local_col] * cell_size,
                                biomass_block[local_row, local_col],
                                lat_coord, lng_coord,
                                ecoregion_lookup[ecoregion_id]['ECO_NAME'],
                                ecoregion_lookup[ecoregion_id]['ECODE_NAME'],
                                ecoregion_lookup[ecoregion_id]['WWF_MHTNAM']))
                            for global_grid_resolution in GRID_RESOLUTION_LIST:
                                #output a grid coordinate in the form
                                #'grid_row-grid_col'
                                grid_row = (
                                    int((geo_trans[3] - row_coord) /
                                        (global_grid_resolution*1000)))
                                grid_col = (
                                    int((col_coord - geo_trans[0]) /
                                        (global_grid_resolution*1000)))
                                grid_id = str(grid_row) + '-' + str(grid_col)
                                outfile.write(";%s" % grid_id)
                            outfile.write('\n')
        outfile.close()

    def output(self):
        return luigi.LocalTarget(BIOMASS_STATS_URI)

class CalculateTotalPrecip(luigi.Task):
    """Luigi task to take the 12 months of precipitation data and calculate
        the dry season length and total precipitation.

        A month is included in the dry season if there is less than 60mm of
        precipitation that month"""

    def requires(self):
        yield IntersectBiomassTask()

    def run(self):
        precip_ds = gdal.Open(GLOBAL_PRECIP_URI)
        base_band = precip_ds.GetRasterBand(1)
        block_size = base_band.GetBlockSize()
        #this is the nodata value of the 12 band raster I got from the
        #ORNL website
        nodata = -99
        band_list = [precip_ds.GetRasterBand(index+1) for index in xrange(12)]

        total_precip_ds = raster_utils.new_raster_from_base(
            precip_ds, TOTAL_PRECIP_URI, 'GTiff', nodata,
            gdal.GDT_Float32)
        total_precip_band = total_precip_ds.GetRasterBand(1)

        dry_season_length_ds = raster_utils.new_raster_from_base(
            precip_ds, DRY_SEASON_LENGTH_URI, 'GTiff', nodata,
            gdal.GDT_Float32)
        dry_season_length_band = dry_season_length_ds.GetRasterBand(1)
        n_cols = dry_season_length_band.XSize
        n_rows = dry_season_length_band.YSize
        cols_per_block, rows_per_block = block_size[0], block_size[1]
        n_col_blocks = int(math.ceil(n_cols / float(cols_per_block)))
        n_row_blocks = int(math.ceil(n_rows / float(rows_per_block)))
        for row_block_index in xrange(n_row_blocks):
            row_offset = row_block_index * rows_per_block
            row_block_width = min(n_rows - row_offset, rows_per_block)

            for col_block_index in xrange(n_col_blocks):
                col_offset = col_block_index * cols_per_block
                col_block_width = min(n_cols - col_offset, cols_per_block)

                array_list = []
                for band in band_list:
                    array_list.append(band.ReadAsArray(
                        xoff=col_offset, yoff=row_offset,
                        win_xsize=col_block_width,
                        win_ysize=row_block_width))
                valid_mask = array_list[0] != nodata
                dry_season_length = numpy.zeros(array_list[0].shape)
                total_precip = numpy.zeros(array_list[0].shape)
                for array in array_list:
                    dry_season_length += array < 60 #dry season less than 60mm
                    total_precip += array
            dry_season_length[~valid_mask] = nodata
            total_precip[~valid_mask] = nodata
            dry_season_length_band.WriteArray(
                dry_season_length, xoff=col_offset, yoff=row_offset)
            total_precip_band.WriteArray(
                total_precip, xoff=col_offset, yoff=row_offset)

        total_precip_band.FlushCache()
        total_precip_band = None
        gdal.Dataset.__swig_destroy__(total_precip_ds)
        total_precip_ds = None
        dry_season_length_band.FlushCache()
        dry_season_length_band = None
        gdal.Dataset.__swig_destroy__(dry_season_length_ds)
        dry_season_length_ds = None

        _align_raster_with_biomass(
            DRY_SEASON_LENGTH_URI, ALIGNED_DRY_SEASON_LENGTH_URI)
        _align_raster_with_biomass(TOTAL_PRECIP_URI, ALIGNED_TOTAL_PRECIP_URI)

    def output(self):
        return [
            luigi.LocalTarget(ALIGNED_TOTAL_PRECIP_URI),
            luigi.LocalTarget(ALIGNED_DRY_SEASON_LENGTH_URI)]


def create_grid(base_uri, start_point, cell_size, x_len, y_len, out_uri):
    """Create an OGR shapefile where the geometry is a set of lines

        base_uri - a gdal dataset to use in creating the output shapefile
            (required)
        start_point - a tuple of floats indicating the upper left corner of the
            grid
        cell_size - a float value for the length of the line segment
        x_len - number of x cells wide
        y_len - number of y cells high
        out_uri - a string representing the file path to disk for the new
            shapefile (required)

        return - nothing"""

    base_ds = gdal.Open(base_uri)
    output_wkt = base_ds.GetProjection()
    output_sr = osr.SpatialReference()
    output_sr.ImportFromWkt(output_wkt)

    if os.path.isfile(out_uri):
        os.remove(out_uri)

    driver = ogr.GetDriverByName('ESRI Shapefile')
    datasource = driver.CreateDataSource(out_uri)

    # Create the layer name from the uri paths basename without the extension
    uri_basename = os.path.basename(out_uri)
    layer_name = os.path.splitext(uri_basename)[0].encode("utf-8")
    grid_layer = datasource.CreateLayer(
            layer_name, output_sr, ogr.wkbPolygon)

    # Add a single ID field
    field = ogr.FieldDefn('gridid', ogr.OFTString)
    grid_layer.CreateField(field)

    for col_index in xrange(x_len):
        for row_index in xrange(y_len):
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddPoint(
                start_point[0] + cell_size * col_index,
                start_point[1] - cell_size * row_index)
            poly.AddPoint(
                start_point[0] + cell_size * (col_index+1),
                start_point[1] - cell_size * row_index)
            poly.AddPoint(
                start_point[0] + cell_size * (col_index+1),
                start_point[1] - cell_size * (row_index+1))
            poly.AddPoint(
                start_point[0] + cell_size * col_index,
                start_point[1] - cell_size * (row_index+1))

            feature = ogr.Feature(grid_layer.GetLayerDefn())
            feature.SetGeometry(poly)
            feature.SetField(0, "%d-%d" % (row_index, col_index))
            grid_layer.CreateFeature(feature)

    datasource.SyncToDisk()
    datasource = None


class MakeGridShapefile(luigi.Task):
    """A luigi task to make a shapefile that grids the global biomass raster
        in equally spaced squares defined by the GRID_RESOLUTION_LIST
        parameter"""
    grid_table_file_list = [
        os.path.join(OUTPUT_DIR, 'grid_stats_%d.csv' % resolution)
        for resolution in GRID_RESOLUTION_LIST]

    shapefile_output_list = [
        os.path.join(OUTPUT_DIR, 'grid_shape_%d.shp' % resolution)
        for resolution in GRID_RESOLUTION_LIST]

    base_uri = GLOBAL_BIOMASS_URI

    def requires(self):
        return ProcessGridCellLevelStats()


    def run(self):
        _, n_cols = raster_utils.get_row_col_from_uri(self.base_uri)

        base_ds = gdal.Open(self.base_uri, gdal.GA_ReadOnly)
        geo_trans = base_ds.GetGeoTransform()
        output_sr = osr.SpatialReference(base_ds.GetProjection())
        #got this from reading the grid output
        string_args = [
            'Confidence', 'gridID', 'forest', 'main_biome', 'main_ecoregion',
            'Continent']
        for global_grid_resolution, grid_filename, shapefile_filename in \
                zip(GRID_RESOLUTION_LIST, self.grid_table_file_list,
                    self.shapefile_output_list):

            if os.path.isfile(shapefile_filename):
                os.remove(shapefile_filename)

            driver = ogr.GetDriverByName('ESRI Shapefile')
            datasource = driver.CreateDataSource(shapefile_filename)

            #Create the layer name from the uri paths basename without the
            #extension
            uri_basename = os.path.basename(shapefile_filename)
            layer_name = os.path.splitext(uri_basename)[0].encode("utf-8")
            grid_layer = datasource.CreateLayer(
                layer_name, output_sr, ogr.wkbPolygon)

            grid_file = open(grid_filename, 'rU')
            headers = grid_file.readline().rstrip().split(',')

            # Add a single ID field
            field = ogr.FieldDefn(headers[0], ogr.OFTString)
            grid_layer.CreateField(field)
            field_names = [headers[0]]
            for arg in headers[1:]:

                if arg.startswith('anthrome_'):
                    arg = 'anth' + arg[9:]
                elif arg.startswith('prop_main'):
                    arg = 'pr_mn' + arg[9:14]
                else:
                    arg = arg[:10]

                if arg in string_args:
                    field = ogr.FieldDefn(arg, ogr.OFTString)
                else:
                    field = ogr.FieldDefn(arg, ogr.OFTReal)
                field_names.append(arg)
                grid_layer.CreateField(field)
            grid_layer.CommitTransaction()

            for line in grid_file:
                gridid = line.split(',')[0]
                lat_coord = int(gridid.split('-')[0])
                lng_coord = int(gridid.split('-')[1])

                ring = ogr.Geometry(ogr.wkbLinearRing)
                ring.AddPoint(
                    lng_coord * (global_grid_resolution * 1000) + geo_trans[0],
                    -lat_coord * (global_grid_resolution * 1000) + geo_trans[3])
                ring.AddPoint(
                    lng_coord * (global_grid_resolution * 1000) + geo_trans[0],
                    -(1+lat_coord) * (global_grid_resolution * 1000) +
                    geo_trans[3])
                ring.AddPoint(
                    (1+lng_coord) * (global_grid_resolution * 1000) +
                    geo_trans[0], -(1+lat_coord) *
                    (global_grid_resolution * 1000) + geo_trans[3])
                ring.AddPoint(
                    (1+lng_coord) * (global_grid_resolution * 1000) +
                    geo_trans[0], -lat_coord * (global_grid_resolution * 1000) +
                    geo_trans[3])
                ring.AddPoint(
                    lng_coord * (global_grid_resolution * 1000) + geo_trans[0],
                    -lat_coord * (global_grid_resolution * 1000) + geo_trans[3])

                poly = ogr.Geometry(ogr.wkbPolygon)
                poly.AddGeometry(ring)


                feature = ogr.Feature(grid_layer.GetLayerDefn())
                feature.SetGeometry(poly)
                #feature.SetField(0, gridid)
                for value, field_name in zip(
                        line.rstrip().split(','), field_names):
                    if field_name in string_args:
                        if value == '-9999':
                            value = 'NA'
                        feature.SetField(field_name, str(value))
                    else:
                        try:
                            feature.SetField(field_name, float(value))
                        except ValueError:
                            feature.SetField(field_name, -9999)
                grid_layer.CreateFeature(feature)

            datasource.SyncToDisk()
            datasource = None

    def output(self):
        return [luigi.LocalTarget(uri) for uri in self.shapefile_output_list]


class ProcessGridCellLevelStats(luigi.Task):
    """Luigi task to loop along the global grid cells and process statistics
        about the biophysical layers underdeath and report to a csv file"""
    grid_output_file_list = [
        os.path.join(OUTPUT_DIR, 'grid_stats_%d.csv' % resolution)
        for resolution in GRID_RESOLUTION_LIST]
    forest_only_table_uri = os.path.join(
        DATA_DIR, "left_join_tables", "forest_only.csv")

    def requires(self):
        for uri in LAYERS_TO_AVERAGE + LAYERS_TO_MAX:
            yield AlignLayerWithBiomassTask(uri)

        yield CalculateTotalPrecip()
        yield IntersectBiomassTask()

    def run(self):
        biomass_ds = gdal.Open(GLOBAL_BIOMASS_URI, gdal.GA_ReadOnly)
        n_rows, n_cols = raster_utils.get_row_col_from_uri(GLOBAL_BIOMASS_URI)

        base_srs = osr.SpatialReference(biomass_ds.GetProjection())
        lat_lng_srs = base_srs.CloneGeogCS()
        coord_transform = osr.CoordinateTransformation(
            base_srs, lat_lng_srs)
        geo_trans = biomass_ds.GetGeoTransform()
        biomass_band = biomass_ds.GetRasterBand(1)
        biomass_nodata = biomass_band.GetNoDataValue()

        forest_table = raster_utils.get_lookup_from_csv(
            self.forest_only_table_uri, 'gridID')
        forest_headers = list(forest_table.values()[0].keys())

        nonexistant_files = []
        for uri in ALIGNED_LAYERS_TO_AVERAGE:
            if not os.path.isfile(uri):
                nonexistant_files.append(uri)
        if len(nonexistant_files) > 0:
            raise Exception(
                "The following files don't exist: %s" %
                (str(nonexistant_files)))

        average_dataset_list = [
            gdal.Open(uri) for uri in ALIGNED_LAYERS_TO_AVERAGE]

        average_band_list = [ds.GetRasterBand(1) for ds in average_dataset_list]
        average_nodata_list = [
            band.GetNoDataValue() for band in average_band_list]

        max_dataset_list = [gdal.Open(uri) for uri in ALIGNED_LAYERS_TO_MAX]
        max_band_list = [ds.GetRasterBand(1) for ds in max_dataset_list]
        max_nodata_list = [band.GetNoDataValue() for band in max_band_list]

        for global_grid_resolution, grid_output_filename in \
                zip(GRID_RESOLUTION_LIST, self.grid_output_file_list):
            try:
                grid_output_file = open(grid_output_filename, 'w')
                grid_output_file.write('grid id,lat_coord,lng_coord')
                for filename in (
                        ALIGNED_LAYERS_TO_AVERAGE + ALIGNED_LAYERS_TO_MAX):
                    grid_output_file.write(
                        ',%s' % os.path.splitext(
                            os.path.basename(filename))[0][len('aligned_'):])
                for header in forest_headers:
                    grid_output_file.write(',%s' % header)
                grid_output_file.write('\n')

                n_grid_rows = int(
                    (-geo_trans[5] * n_rows) / (global_grid_resolution * 1000))
                n_grid_cols = int(
                    (geo_trans[1] * n_cols) / (global_grid_resolution * 1000))

                grid_row_stepsize = int(n_rows / float(n_grid_rows))
                grid_col_stepsize = int(n_cols / float(n_grid_cols))

                for grid_row in xrange(n_grid_rows):
                    for grid_col in xrange(n_grid_cols):
                        #first check to make sure there is biomass at all!
                        global_row = grid_row * grid_row_stepsize
                        global_col = grid_col * grid_col_stepsize
                        global_col_size = min(
                            grid_col_stepsize, n_cols - global_col)
                        global_row_size = min(
                            grid_row_stepsize, n_rows - global_row)
                        array = biomass_band.ReadAsArray(
                            global_col, global_row, global_col_size,
                            global_row_size)
                        if numpy.count_nonzero(array != biomass_nodata) == 0:
                            continue

                        grid_id = '%d-%d' % (grid_row, grid_col)
                        grid_row_center = (
                            -(grid_row + 0.5) * (global_grid_resolution*1000) +
                            geo_trans[3])
                        grid_col_center = (
                            (grid_col + 0.5) * (global_grid_resolution*1000) +
                            geo_trans[0])
                        grid_lng_coord, grid_lat_coord, _ = (
                            coord_transform.TransformPoint(
                                grid_col_center, grid_row_center))
                        grid_output_file.write(
                            '%s,%s,%s' % (grid_id, grid_lat_coord,
                                          grid_lng_coord))


                        #take the average values
                        for band, nodata, layer_uri in zip(
                                average_band_list, average_nodata_list,
                                ALIGNED_LAYERS_TO_AVERAGE +
                                ALIGNED_LAYERS_TO_MAX):
                            nodata = band.GetNoDataValue()
                            array = band.ReadAsArray(
                                global_col, global_row, global_col_size,
                                global_row_size)
                            layer_name = os.path.splitext(
                                os.path.basename(layer_uri)) \
                            [0][len('aligned_'):]

                            pure_average_layers = [
                                'global_elevation', 'global_water_capacity',
                                'fi_average', 'lighted_area_luminosity',
                                'glbctd1t0503m', 'glbgtd1t0503m',
                                'glbpgd1t0503m', 'glbshd1t0503m', 'glds00ag',
                                'glds00g']
                            if layer_name not in pure_average_layers:
                                array[array == nodata] = 0.0
                            valid_values = array[array != nodata]
                            if valid_values.size != 0:
                                value = numpy.average(valid_values)
                            else:
                                value = -9999.
                            grid_output_file.write(',%f' % value)

                        #take the mode values
                        for band, nodata in zip(max_band_list, max_nodata_list):
                            nodata = band.GetNoDataValue()
                            array = band.ReadAsArray(
                                global_col, global_row, global_col_size,
                                global_row_size)
                            #get the most common value
                            valid_values = array[array != nodata]
                            if valid_values.size != 0:
                                value = scipy.stats.mode(valid_values)[0][0]
                                grid_output_file.write(',%f' % value)
                            else:
                                grid_output_file.write(',-9999')

                        #add the forest_only values
                        for header in forest_headers:
                            try:
                                value = forest_table[grid_id][header]
                                if type(value) == unicode:
                                    grid_output_file.write(
                                        ',%s' % forest_table[grid_id][header].\
                                        encode('latin-1', 'replace'))
                                else:
                                    grid_output_file.write(
                                        ',%s' % forest_table[grid_id][header])
                            except KeyError:
                                grid_output_file.write(',-9999')


                        grid_output_file.write('\n')
                grid_output_file.close()
            except IndexError as exception:
                grid_output_file.close()
                os.remove(grid_output_filename)
                raise exception

    def output(self):
        return [luigi.LocalTarget(uri) for uri in self.grid_output_file_list]


class Runit(luigi.Task):
    """Main entry Luigi point"""
    def requires(self):
        return [
            ProcessEcoregionTask(), ProcessGridCellLevelStats(),
            MakeGridShapefile()]


if __name__ == '__main__':
    raster_utils.create_directories([OUTPUT_DIR])
    luigi.run()
