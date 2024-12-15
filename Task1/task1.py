from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
import csv

# source for regex: https://docs.python.org/3/library/re.html
# It extracts words containing only letters.
WORD_RE = re.compile(r'\b[a-zA-Z]+\b')

# source for stopwords: https://www.geeksforgeeks.org/removing-stop-words-nltk-python/
# It excludes words like "the", "is", etc.
STOPWORDS = set(stopwords.words('english'))


"""
    This class helps to find the top 10 most common keywords within movie titles for each possible genre using MapReduce.
"""
class MovieGenreKeywordCount(MRJob):
    """
        This mapper method yields key-value pairs: ((genre, keyword), 1)
    """
    def mapper(self, _, line):
        # source to read csv file: https://docs.python.org/3/library/csv.html
        reader = csv.reader([line])
        row = next(reader)  # Read the line as a row
        if row[0] == "movieId":  # It skips the header row
            return

        title = row[1] # It exracts movie title
        genres = row[2] # It exracts movie genre

        # Split genres and it turns to a list
        genres = genres.split('|')
        
        # It converts keywords to lowercase and excludes stopwords, extracting meaningful keywords from the title.
        keywords = [keyword.lower() for keyword in WORD_RE.findall(title) if keyword.lower() not in STOPWORDS]
        
        for genre in genres:
            if genre != "(no genres listed)": # This excludes genres which are not listed.
                for keyword in keywords:
                    yield (genre, keyword), 1 # key = (genre, keyword), value = [1]

    """
        Combiner method used to sum up intermediate counts
        It yields ([genre, word], sum(counts))
    """              
    def combiner(self, genre_keyword, counts): # key = genre_keyword(genre, keyword), value = counts
        yield genre_keyword, sum(counts) 

    """
         This reducer_count method combines counts grouped by (genre, keyword)
         It yields (genre, (keyword, total_count))
    """
    def reducer_count(self, genre_keyword, counts):
        genre, keyword = genre_keyword
        yield genre, (keyword, sum(counts))
    
    """
        This helper method extracts the count from each tuple (keyword, count)
    """
    def get_count(self,keyword_count):
        return keyword_count[1]

    """
        This reducer method gives only top 10 keywords for each genre
    """
    def reducer_top10(self, genre, keyword_counts):
        # it sorts word counts by frequency and select the top 10
        top_10 = sorted(keyword_counts, key = self.get_count, reverse = True)[0:10]
        yield genre, top_10
    
    """
        This method defines the sequence of MapReduce steps.
    """
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer_count
            ),
            MRStep(
                reducer=self.reducer_top10
            )
        ]   


if __name__ == '__main__':
    MovieGenreKeywordCount.run()
