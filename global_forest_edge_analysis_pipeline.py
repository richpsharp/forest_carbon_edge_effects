import os

import gdal
import numpy
import luigi

from invest_natcap import raster_utils

WORKSPACE = "C:/Users/rpsharp/Documents/carbon_edge_pipeline_workspace"

class UnionBiomassMaps(luigi.Task):

	output_uri = os.path.join(WORKSPACE, "union_biomass.tif")

	def output(self):
		return luigi.LocalTarget(self.output_uri)

	def run(self):
		biomass_raster_list = [
			"C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/af_biov2ct1.tif",
			"C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/am_biov2ct1.tif",
			"C:/Users/rpsharp/Dropbox_stanford/Dropbox/forest_edge_carbon/as_biov2ct1.tif",
		]

		nodata = raster_utils.get_nodata_from_uri(biomass_raster_list[0])
		cell_size = raster_utils.get_cell_size_from_uri(biomass_raster_list[0])

		def union_op(*biomass_array_list):
			output_array = biomass_array_list[0]
			for biomass_array in biomass_array_list[1:]:
				output_array = numpy.where(
					biomass_array != nodata, biomass_array, output_array)
			return output_array

		raster_utils.create_directories([os.path.dirname(self.output_uri)])

		raster_utils.vectorize_datasets(
	        biomass_raster_list, union_op, self.output_uri, gdal.GDT_Int16,
	        nodata, cell_size, 'union', dataset_to_align_index=0,
	        vectorize_op=False)

if __name__ == '__main__':
	
    luigi.run()
