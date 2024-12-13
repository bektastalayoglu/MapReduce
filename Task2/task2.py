# Google web graph

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# source for regex: https://docs.python.org/3/library/re.html
MATCH_RE = re.compile(r"^(?!#).*")

class ReverseWebLinkGraph(MRJob):

    # mapper creates a list of source_id and target_id web pages for each line
    # and it returns the reversed list
    def mapper(self, _, line):
        nodes = MATCH_RE.findall(line)
        if nodes:
            nodes = nodes[0].split('\t')
            source_node = nodes[0]
            target_node = nodes[1]
            yield target_node, source_node

    # reducer returns a list of sources for each target
    def reducer(self, target_node, source_node):
        yield target_node, list(source_node)

    def steps(self):
        return [
            MRStep(mapper=self.mapper),
            MRStep(reducer=self.reducer)
        ]


if __name__ == '__main__':
    ReverseWebLinkGraph.run()