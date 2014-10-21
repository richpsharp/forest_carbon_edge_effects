import os

import ogr

from invest_natcap import raster_utils


def annotate_ecoregions(ecoregion_uri, table_uri):
    print 'generating report'
    esri_driver = ogr.GetDriverByName('ESRI Shapefile')

    original_datasource = ogr.Open(ecoregion_uri)
    updated_datasource_uri = os.path.join(os.path.dirname(ecoregion_uri), 'annotated_ecoregions.shp')
    #If there is already an existing shapefile with the same name and path, delete it
    #Copy the input shapefile into the designated output folder
    if os.path.isfile(updated_datasource_uri):
        os.remove(updated_datasource_uri)
    datasource_copy = esri_driver.CopyDataSource(original_datasource, updated_datasource_uri)
    layer = datasource_copy.GetLayer()

    new_field_names = [('(core-edge)/core', 'magnitude'), ('A(80)', 'A80'), ('A(90)', 'A90'), ('A(95)', 'A95')]

    lookup_table = raster_utils.get_lookup_from_csv(table_uri, 'region')


    for table_field, field_name in new_field_names:
        field_def = ogr.FieldDefn(field_name, ogr.OFTReal)
        layer.CreateField(field_def)

    for feature_id in xrange(layer.GetFeatureCount()):
        feature = layer.GetFeature(feature_id)

        feature_eco_name = feature.GetField('ECO_NAME')
        for table_field, field_name in new_field_names:
            try:
                value = lookup_table[feature_eco_name][table_field]
                feature.SetField(field_name, float(value))
            except KeyError:
                feature.SetField(field_name, -1.0)
            except TypeError:
                feature.SetField(field_name, -1.0)

        #Save back to datasource
        layer.SetFeature(feature)


if __name__ == '__main__':
    ecoregion_uri = "C:/Users/rich/Desktop/ecoregions/ecoregions_projected.shp"
    table_uri = "C:/Users/rich/Documents/Dropbox/Edge Carbon Effects/Spreadsheets/edge_effects_forest_ecoregions.csv"
    annotate_ecoregions(ecoregion_uri, table_uri)
