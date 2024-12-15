# Google web graph

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# source for regex: https://docs.python.org/3/library/re.html
# Do not start with # commented lines are ignored 
MATCH_RE = re.compile(r"^(?!#).*")

"""
    This class reverse the web-link graph from the given Google web graph file
    Outputs a reversed graph: target -> source
"""
class ReverseWebLinkGraph(MRJob):

    """ 
    This mapper method creates a list of source node and target node for each line
        Input Parameters:
        - line: It is the single line from the dataset
        Output:
        - It yields the reversed key-value pairs: (target_node, source_node) 
    """
    def mapper(self, _, line):
        # Commented lines are ignored 
        nodes = MATCH_RE.findall(line)
        if nodes:
            # Split the line to separate source and target nodes
            nodes = nodes[0].split()
            source_node = nodes[0] # Source node
            target_node = nodes[1] # Target node
            yield target_node, source_node 

    """ 
    This reducer method
        Input Parameters:
        - target_node : key from mapper
        - source_node : value from mapper
        Output:
        - It yields a list of sources for each target 
    """

    def reducer(self, target_node, source_node):
        yield target_node, list(source_node)

    """ 
    This method defines the sequence of MapReduce steps 
    """
    def steps(self):
        return [
            MRStep(mapper=self.mapper),
            MRStep(reducer=self.reducer)
        ]


if __name__ == '__main__':
    ReverseWebLinkGraph.run()