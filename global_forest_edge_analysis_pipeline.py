import os
import shutil

import dill as pickle
import gdal
import numpy
import luigi

from invest_natcap import raster_utils

WORKSPACE = "C:/Users/rpsharp/Documents/carbon_edge_pipeline_workspace"
UNION_BIOMASS_URI = os.path.join(WORKSPACE, "union_biomass.tif")
UNION_LANDCOVER_URI = os.path.join(WORKSPACE, "union_landcover.tif")
INTERSECT_BIOMASS_URI = os.path.join(WORKSPACE, "intersect_biomass.tif")
INTERSECT_LANDCOVER_URI = os.path.join(WORKSPACE, "intersect_landcover.tif")

BIOMASS_RASTER_LIST = [
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/af_biov2ct1.tif",
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/am_biov2ct1.tif",
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/as_biov2ct1.tif",
]
LANDCOVER_RASTER_LIST = [
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/af.tif",
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/as.tif",
    "C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/am.tif"
]

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

class Runit(luigi.Task):
    def requires(self):
        return [IntersectLandcoverTask(), IntersectBiomassTask()]


if __name__ == '__main__':
    print '\n' * 10
    if os.path.exists(WORKSPACE):
        #shutil.rmtree(WORKSPACE)
        pass
    #runit = Runit()
    #scheduler = scheduler.CentralPlannerScheduler()
    #for _ in range(4):
    #    scheduler.add_worker(worker.Worker())

    raster_utils.create_directories([WORKSPACE])
    luigi.run()
