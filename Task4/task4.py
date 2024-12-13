from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class FrobeniusNormMR(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_sum_of_squares,
                   reducer=self.reducer_row_sum),
            MRStep(reducer=self.reducer_frobenius_norm)
        ]

    def mapper_sum_of_squares(self, _, line):
        # Parse each row of the matrix
        numbers = list(map(float, line.strip().split()))
        # Calculate the sum of squares for this row
        row_sum_of_squares = 0
        for num in numbers:
            row_sum_of_squares += num ** 2  # Add the square of each number
        # Emit intermediate result
        yield None, row_sum_of_squares

    def reducer_row_sum(self, _, row_sums):
        # Aggregate the sum of squares from all rows
        total_sum_of_squares = 0
        for value in row_sums:
            total_sum_of_squares += value
        # Emit the total sum of squares
        yield None, total_sum_of_squares

    def reducer_frobenius_norm(self, _, total_sums):
        # Calculate the Frobenius norm (square root of the total sum of squares)
        frobenius_norm = np.sqrt(sum(total_sums))
        yield "Frobenius Norm", frobenius_norm

if __name__ == '__main__':
    FrobeniusNormMR.run()
