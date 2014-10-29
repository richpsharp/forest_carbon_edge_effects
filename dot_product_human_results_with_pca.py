import csv
import numpy

pca_file = open('data.txt', 'r')

value_list = []
for line in pca_file:
    values =  line.split()
    value_list.append(numpy.array([float(x) for x in values[1:]]))

pca_matrix = numpy.array(value_list)

print pca_matrix.shape

human_use_file = open(
    "C:/Users/rich/Documents/Dropbox/Edge Carbon Effects/carbon/all_grid_results_100km_human_v2.csv", 'rU')
human_use_out_file = open(
    "C:/Users/rich/Documents/Dropbox/Edge Carbon Effects/carbon/all_grid_results_100km_human_v3.csv", 'w')
headers = human_use_file.readline()
human_use_out_file.write(headers.rstrip() + ',' + ','.join(['PC%d' % id for id in range(1, 6)]) + '\n')
human_use_csv = csv.reader(human_use_file)
for line in human_use_csv:
    #print line
    human_use_out_file.write(','.join(line))
    row = numpy.array([float(x) for x in line[-28:]])
    for id in range(5):
        human_use_out_file.write(',' + str(sum(row * pca_matrix[:,id])))
    human_use_out_file.write('\n')
    
    