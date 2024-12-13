from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import math
import csv

class KNN_Neighbour(MRJob):
    # It's been mentioned that number of neighbours needs to be 15
    K=15

    # (https://stackoverflow.com/questions/50420393/mrjob-add-file-arg-csv-file)
    def configure_args(self):
        super(KNN_Neighbour, self).configure_args()
        self.add_file_arg('--iris-data', help='Path to Iris dataset')

    def load_iris(self):
        #Load known samples and tries to parse this part
        self.known_samples = []
        with open(self.options.iris_data, 'r') as file:
            reader = csv.reader(file)
            # First row is header and needs to be skipped
            next(reader)  
            for row in reader:
                # Unpaack the values into variables
                samp_id, s_l, s_w, p_l, p_w, species = row
                # Only known samples are needed
                if species:  
                    self.known_samples.append({
                        'id': samp_id,
                        'features': [float(s_l), float(s_w), float(p_l), float(p_w)],
                        'species': species
                    })

    # inspired by (https://buildprojectswithmayur.hashnode.dev/scaling-knn-using-mapreduce)
    def mapper_init(self):
        # iris is initialised
        self.load_iris()

    def mapper(self, _, line):
        # you can calculate the distance of known and unkonwn samples here
        reader=csv.reader([line])
        for row in reader:
            samp_id, s_l, s_w, p_l, p_w, species = row
            # Only unknown samples are needed
            if not species:  
                unknown_feats = np.array([float(s_l), float(s_w), float(p_l), float(p_w)])

            # Now let's calculate the Euclidean distance
                for items in self.known_samples:
                    known_feats=items['features']
                    
                    # https://numpy.org/doc/stable/reference/generated/numpy.linalg.norm.html
                    # this np.linalg.norm calculate the Euclidean distance efficiently
                    distance=np.linalg.norm(unknown_feats - known_feats)
                    yield samp_id, (distance, items['species'])
    
    def reducer(self, unknown_item, distances):
        # you can now classify based on nearest neighbours

        # Sort by distance
        sorted_distances = sorted(distances, key=lambda x: x[0])
        # Select the top K neighbors
        nearest_neighbor = sorted_distances[:self.K]

        # Count species frequencies
        species_count = {}
        for _, species in nearest_neighbor:
            species_count[species] = species_count.get(species, 0) + 1
        
        # Determine the majority class
        predicted_species = max(species_count, key=species_count.get)
        yield unknown_item, predicted_species

if __name__ == '__main__':
    KNN_Neighbour.run()

# python KNN-neighbours.py --iris-data=Iris.csv Iris.csv>>knn.txt