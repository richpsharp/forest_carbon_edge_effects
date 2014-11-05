import os
import shutil

import dill as pickle
import gdal
import numpy
import luigi

from invest_natcap import raster_utils

DATA_DIR = "F:/Dropbox/forest_edge_carbon_data"
OUTPUT_DIR = "E:/carbon_edge_pipeline"
UNION_BIOMASS_URI = os.path.join(OUTPUT_DIR, "union_biomass.tif")
UNION_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "union_landcover.tif")
INTERSECT_BIOMASS_URI = os.path.join(OUTPUT_DIR, "intersect_biomass.tif")
INTERSECT_LANDCOVER_URI = os.path.join(OUTPUT_DIR, "intersect_landcover.tif")

BIOMASS_RASTER_LIST = [os.path.join(DATA_DIR, '%s_biov2ct1.tif' % prefix) for prefix in ['af', 'am', 'as']]
LANDCOVER_RASTER_LIST = [os.path.join(DATA_DIR, '%s.tif' % prefix) for prefix in ['af', 'am', 'as']]


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
            INTERSECT_BIOMASS_URI,
            gdal.GDT_Int16, nodata, cell_size, "intersection",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(INTERSECT_BIOMASS_URI)


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
            INTERSECT_LANDCOVER_URI,
            gdal.GDT_Int16, nodata, cell_size, "intersection",
            dataset_to_align_index=0, vectorize_op=False)

    def output(self):
        return luigi.LocalTarget(INTERSECT_LANDCOVER_URI)

class RasterizeEcoregion(luigi.Task):
    ecoregion_dataset_uri = os.path.join(OUTPUT_DIR, "ecoregion_id.tif")

    def requires(self):
        return [IntersectBiomassTask(), IntersectLandcoverTask()]

    def run(self):
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

        #create ecoregion id
        raster_utils.new_raster_from_base_uri(
            INTERSECT_LANDCOVER_URI, self.ecoregion_dataset_uri, 'GTiff',
            ecoregion_nodata, gdal.GDT_Int16)
        raster_utils.rasterize_layer_uri(
            self.ecoregion_dataset_uri, ecoregion_shapefile_uri,
            option_list=["ATTRIBUTE=ECO_ID_U"])

    def output(self):
        return luigi.LocalTarget(self.ecoregion_dataset_uri)


class CalculateForestEdge(luigi.Task):
    forest_edge_distance_uri = os.path.join(
            OUTPUT_DIR, "forest_edge_distance.tif")

    def requires(self):
        return IntersectLandcoverTask()

    def run(self):
        lulc_nodata = raster_utils.get_nodata_from_uri(INTERSECT_LANDCOVER_URI)

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

        cell_size = raster_utils.get_cell_size_from_uri(INTERSECT_LANDCOVER_URI)
        raster_utils.vectorize_datasets(
            [INTERSECT_LANDCOVER_URI,], mask_nonforest, mask_uri, gdal.GDT_Byte,
            mask_nodata, cell_size, 'intersection', dataset_to_align_index=0,
            dataset_to_bound_index=None, aoi_uri=None,
            assert_datasets_projected=True, process_pool=None,
            vectorize_op=False, datasets_are_pre_aligned=True)

        raster_utils.distance_transform_edt(
            mask_uri, self.forest_edge_distance_uri)

    def output(self):
        return luigi.LocalTarget(self.forest_edge_distance_uri)


class ProcessEcoregionTask(luigi.Task):
    biomass_stats_uri = os.path.join(OUTPUT_DIR, "biomass_stats.csv")

    def requires(self):
        return [CalculateForestEdge(), RasterizeEcoregion()]

    def run(self):
        pass
        #_aggregate_results(
        #    forest_edge_distance_uri, biomass_uri, ecoregion_dataset_uri,
        #    ecoregion_lookup, biomass_stats_uri)

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
