import numpy as np
import pandas as pd
from mrjob.job import MRJob
import csv


# Load and preprocess the dataset
file_path = 'Iris.csv'  # Adjust the path if necessary
data = pd.read_csv(file_path)

# Extract feature columns (exclude ID and Species)
features = data.columns[1:-1]
# Extract values of the features
feature_val = data.loc[:, features].values

# Normalize feature values using min-max normalization
min_values = feature_val.min(axis=0) # Finds the minimum value of each feature
max_values = feature_val.max(axis=0) # Finds the maximum value of each feature
features_normalized = (feature_val - min_values) / (max_values - min_values)

# Creates a normalized dataframe
features_normalized_df = pd.DataFrame(features_normalized, columns=features)

# We add back id and species columns
features_normalized_df.insert(0, 'Id', data['Id'])
features_normalized_df['Species'] = data['Species']

# Split the data into known and unknown samples for use in the next steps, 
# and convert it into a dictionary to simplify access to features.
# source: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html
known_data = features_normalized_df.dropna(subset=['Species']).to_dict(orient='records')  # Known samples
unknown_data = features_normalized_df[features_normalized_df['Species'].isna()].to_dict(orient='records')  # Unknown samples


"""
    This class implements the k-Nearest Neighbors algorithm using MapReduce.    
"""
class KNNMapReduce(MRJob):
    # Number of neighbors
    K = 15

    """
        This mapper method calculates distances between unknown samples and all known samples.
        Input Parameter:
        - line: A single row of the dataset.
        Output:
        - key: id of the unknown sample.
        - value: tuple containing (distance, known_species).
    """
    def mapper(self, _, line):
        reader = csv.reader([line])
        row = next(reader)  # Read the line
        if row[0] == "Id":  # Skip header row
            return

        # Process each unknown sample
        for unknown_sample in unknown_data:
            unknown_id = unknown_sample['Id']
            unknown_features = np.array([unknown_sample[feature] for feature in features])

            # Calculate distances to all known samples
            for known_sample in known_data:
                known_id = known_sample['Id']
                known_features = np.array([known_sample[feature] for feature in features])
                known_species = known_sample['Species']

                # Calculate Euclidean distance
                # source: https://www.geeksforgeeks.org/calculate-the-euclidean-distance-using-numpy/
                distance = np.linalg.norm(unknown_features - known_features)

                yield unknown_id, (distance, known_species)
    
    """
    Reducer finds the K nearest neighbors and predicts the species for each unknown sample.
    Input Parameters:
    - unknown_id: ID of the unknown sample.
    - distances: An iterable of (distance, species) tuples.
    Output:
    - Key: ID of the unknown sample.
    - Value: Predicted species based on majority vote of K nearest neighbors.
    """    
    def reducer(self, unknown_id, distances):
        # Sort distances 
        sort_distances = sorted(distances, key=lambda x: x[0])
        # Select the top K. self.K = 15
        k_n_n = sort_distances[:self.K]

        # Count the species of the K nearest neighbors
        count = {}
        for _, species in k_n_n:
            count[species] = count.get(species, 0) + 1
        # Predict the species with the highest count
        predicted_species = max(count, key=count.get)
        yield unknown_id, predicted_species


if __name__ == "__main__":
    KNNMapReduce.run()
