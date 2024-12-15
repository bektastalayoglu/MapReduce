from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

"""
    This class helps to calculate the Frobenius norm of a given matrix.
"""

class FrobeniusNormMR(MRJob):
    """
    The mapper method 
        Input Parameters: 
        - line: It is the single line(row) of the matrix
        Output:
        - It yields key-value pair none as key and each element of the row as value.
    """
    def mapper(self, _, line):
        for value in line.split():
            yield None, float(value)

    """
    The reducer method 1:
    It calculates the sum of the squares of the elements of the matrix.
        Input Parameters:
        - values: A list of float values representing matrix elements.
        Output:
        - It yields the total squared sum for all rows.
    """
    def reducer_sum_squares(self, _, values):
        total_sum = 0
        for value in values:
            total_sum += value ** 2
        yield None, total_sum

    """
    The reducer method 2:
    It calculates the Frobenius Norm of the matrix.
        Input Parameters:
        - total_sum: the total sum of squares from the first reducer
        Output:
        - Key: "Frobenius Norm:"
        - Value: Frobenius norm, calculated as the square root of the total sum.
    """
    def reducer_frobenius_norm(self, _, total_sum):
        frobenius_norm = sqrt(sum(total_sum))
        yield "Frobenius Norm:", frobenius_norm

    """
        This method defines the sequence of MapReduce steps.
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_sum_squares),
            MRStep(reducer=self.reducer_frobenius_norm)
        ]
    
if __name__ == '__main__':
    FrobeniusNormMR.run()
