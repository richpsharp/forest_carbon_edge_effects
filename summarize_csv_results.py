import os
import collections
import time

from invest_natcap import raster_utils

CSV_DIR = os.path.join("C:\\", "Users", "rich", "Desktop", "forest_edge_output")

if __name__ == '__main__':
    raster_utils.email_report(
        "starting summarize_csv_results.py", "3152624786@txt.att.net")
    for prefix in ['am', 'af', 'as']:
        biomass_stats_uri = os.path.join(
            CSV_DIR, "%s_biomass_stats.csv" % prefix)
        biomass_file = open(biomass_stats_uri, 'rU')

        print 'calculating lines'
        num_lines = sum(1 for line in open(biomass_stats_uri, 'rU'))
        print 'number of lines: %d' % num_lines
        distance_biomass_lookup = collections.defaultdict(list)
        start_time = time.time()
        count = 0
        for line in biomass_file:
            distance, biomass = [float(x) for x in line.split(',')[0:2]]
            distance_biomass_lookup[distance].append(biomass)
            count += 1
            current_time = time.time()
            if current_time - start_time > 3.0:
                print '%.2f%% complete' % (100.0 * count / float(num_lines))
                start_time = current_time

        biomass_summary_uri = os.path.join(
            CSV_DIR, '%s_biomass_summary.csv' % prefix)
        biomass_summary_file = open(biomass_summary_uri, 'w')
        
        #iterate over increasing distance
        for distance in sorted(distance_biomass_lookup.keys()):
            print 'processing distance %.2f' % distance
            biomass_list = sorted(distance_biomass_lookup[distance])
            biomass_summary_file.write('%f' % distance)
            biomass_summary_file.write(
                ',%f,%f' % (biomass_list[0], biomass_list[-1]))
            biomass_length = len(biomass_list)
            
            #print out a tuple, the lower and upper bound of whatever p_value
            for p_value in [0.01, 0.05, 0.1, 0.5, 1.0]:
                tail_percent = p_value/2.0
                left_index = int(biomass_length * tail_percent)
                right_index = (biomass_length - 1) - left_index
                biomass_summary_file.write(',%f,%f' %
                    (biomass_list[left_index], biomass_list[right_index]))
            #print the average biomass value
            biomass_summary_file.write(
                ',%f' % (sum(biomass_list)/float(len(biomass_list))))
                
            #print the count
            biomass_summary_file.write(',%d\n' % biomass_length)
            
    raster_utils.email_report(
        "done with summarize_csv_results.py", "3152624786@txt.att.net")
