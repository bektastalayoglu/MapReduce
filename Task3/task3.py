import numpy as np
import pandas as pd
from mrjob.job import MRJob
import csv


# Load and preprocess the dataset
file_path = 'Iris.csv'  # Adjust the path if necessary
data = pd.read_csv(file_path)

# Extract feature columns (exclude ID and Species)
features = data.columns[1:-1]
x = data.loc[:, features].values

# Normalize feature values
min_vals = x.min(axis=0)
max_vals = x.max(axis=0)
x_normalized = (x - min_vals) / (max_vals - min_vals)

# Create a normalized DataFrame with features
x_normalized_df = pd.DataFrame(x_normalized, columns=features)

# Add back 'Id' and 'Species' columns
x_normalized_df.insert(0, 'Id', data['Id'])
x_normalized_df['Species'] = data['Species']

# Split data into known and unknown samples
known_data = x_normalized_df.dropna(subset=['Species']).to_dict(orient='records')  # Known samples
unknown_data = x_normalized_df[x_normalized_df['Species'].isna()].to_dict(orient='records')  # Unknown samples


class KNNMapReduce(MRJob):
    # Number of neighbors
    K = 15
    def mapper(self, _, line):
        """Mapper calculates distances for each unknown sample."""
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
                distance = np.linalg.norm(unknown_features - known_features)

                # Emit the unknown sample ID and distance with known species
                yield unknown_id, (distance, known_species)

    def reducer(self, unknown_id, distances):
        """Reducer finds the K nearest neighbors and predicts the species."""
        # Sort distances and select the top K
        sorted_distances = sorted(distances, key=lambda x: x[0])
        nearest_neighbors = sorted_distances[:self.K]

        # Count the species of the K nearest neighbors
        species_count = {}
        for _, species in nearest_neighbors:
            species_count[species] = species_count.get(species, 0) + 1

        # Predict the species with the highest count
        predicted_species = max(species_count, key=species_count.get)
        yield unknown_id, predicted_species


if __name__ == "__main__":
    KNNMapReduce.run()
