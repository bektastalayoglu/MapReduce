from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords
import csv



# source for regex: https://docs.python.org/3/library/re.html
WORD_RE = re.compile(r'\b[a-zA-Z]+\b')

# source for stopwords: https://www.geeksforgeeks.org/removing-stop-words-nltk-python/
STOPWORDS = set(stopwords.words('english'))

class MovieGenreKeywordCount(MRJob):

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
  
    def mapper(self, _, line):
        # source to read csv file: https://docs.python.org/3/library/csv.html
        reader = csv.reader([line])
        row = next(reader)  # Read the line
        if row[0] == "movieId":  # Skip header row
            return

        title = row[1]
        genres = row[2]

        # Split genres and it turns to a
        genres = genres.split('|')
        
        # Extract meaningful keywords from the title
        words = [word.lower() for word in WORD_RE.findall(title) if word.lower() not in STOPWORDS]
        
        for genre in genres:
            if genre != "(no genres listed)":
                for word in words:
                    yield (genre, word), 1 # key = (genre, world), value = [1]

    def combiner(self, genre_word, counts): # key = genre_world, value = counts
        # Sum intermediate counts
        yield genre_word, sum(counts)

    def reducer_count(self, genre_word, counts):
        # Combine counts grouped by (genre, word)
        genre, word = genre_word
        yield genre, (word, sum(counts))
    
    # It extracts the count from each tuple to use as a key 
    def get_count(self,word_count):
        return word_count[1]

    # It gives only top 10 genre keyword
    def reducer_top10(self, genre, word_counts):
        top_10 = sorted(word_counts, key = self.get_count, reverse = True)[0:10]
        yield genre, top_10
    



if __name__ == '__main__':
    MovieGenreKeywordCount.run()
