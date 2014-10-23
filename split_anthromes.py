import os
import numpy
import gdal

from invest_natcap import raster_utils

anthrome_uri = "C:/Users/rich/Desktop/average_layers_projected/gl_anthrome.tif"
anthrome_ds = gdal.Open(anthrome_uri)
anthrome_band = anthrome_ds.GetRasterBand(1)

anthrome_array = anthrome_band.ReadAsArray()
unique_anthrome_ids = numpy.unique(anthrome_array)

for anthrome_id in unique_anthrome_ids:
    output_uri = os.path.join(os.path.dirname(anthrome_uri), 'anthrome_%d.tif' % anthrome_id)
    out_pixel_size = raster_utils.get_cell_size_from_uri(anthrome_uri)
    raster_utils.vectorize_datasets(
        [anthrome_uri], lambda x: x==anthrome_id, output_uri, gdal.GDT_Byte,
        127, out_pixel_size, "union", vectorize_op=False)

print numpy.unique(anthrome_array).shape