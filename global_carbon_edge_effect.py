import os
import sys

import gdal
import osr
import numpy
import pyproj


from invest_natcap import raster_utils

BIOMASS_PREFIXES = ['am', 'af', 'as']
DATA_DIR = os.path.join("C:\\", "Users", "rich", "Desktop")
OUTPUT_DIR = os.path.join("C:\\", "Users", "rich", "Desktop", "forest_edge_output")
BIOMASS_BASE = '_biov2ct1.tif'
LULC_BASE = '.tif'
FOREST_LULCS = [1, 2, 3, 4, 5]


if __name__ == '__main__':
    #mask out forest LULCs
    raster_utils.create_directories([OUTPUT_DIR])
    
    pantropic_regions = ['am', 'af', 'as']
    ecoregion_headers = ['ECO_NAME', 'ECODE_NAME', 'WWF_MHTNAM']
    
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
    
    for prefix in pantropic_regions:
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
            
        ecoregion_dataset = gdal.Open(ecoregion_dataset_uri)
        ecoregion_band = ecoregion_dataset.GetRasterBand(1)
        
        lulc_nodata = raster_utils.get_nodata_from_uri(lulc_uri)
        biomass_nodata = raster_utils.get_nodata_from_uri(biomass_uri)
        
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

        forest_edge_nodata = raster_utils.get_nodata_from_uri(forest_edge_distance_uri)
        biomass_stats_uri = os.path.join(OUTPUT_DIR,  "%s_biomass_stats.csv" % prefix)
        outfile = open(biomass_stats_uri, 'w')
        
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
        
        for row_index in xrange(n_rows):
            print row_index, n_rows
            biomass_row = biomass_band.ReadAsArray(0, row_index, n_cols, 1)
            forest_edge_distance_row = forest_edge_distance_band.ReadAsArray(
                0, row_index, n_cols, 1)
            ecoregion_id_row = ecoregion_band.ReadAsArray(
                0, row_index, n_cols, 1)
            row_coord = gt[3] + row_index * gt[5]
            for col_index in xrange(n_cols):
                col_coord = gt[0] + col_index * gt[1]
                lng_coord, lat_coord, _ = coord_transform.TransformPoint(
                    col_coord, row_coord)
                ecoregion_id = ecoregion_id_row[0, col_index]
                if forest_edge_distance_row[0, col_index] != forest_edge_nodata and forest_edge_distance_row[0, col_index] > 0.0 and biomass_row[0, col_index] != biomass_nodata:
                    outfile.write("%f;%f;%f;%f;%s;%s;%s\n" % (forest_edge_distance_row[0, col_index] * cell_size, biomass_row[0, col_index], lat_coord, lng_coord, ecoregion_lookup[ecoregion_id]['ECO_NAME'], ecoregion_lookup[ecoregion_id]['ECODE_NAME'], ecoregion_lookup[ecoregion_id]['WWF_MHTNAM']))
        
    raster_utils.email_report(
        "done with global_carbon_edge_effect.py", "3152624786@txt.att.net")