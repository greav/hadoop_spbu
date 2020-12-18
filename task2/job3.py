import re

from mrjob.job import MRJob
from mrjob.step import MRStep

WORD_RE = re.compile(r"[A-Za-z]+")


class MRMostFrequentWord(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield None, (sum(counts), word)

    def most_frequent_reducer(self, _, word_count_pairs):
        yield max(word_count_pairs)

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.most_frequent_reducer),
        ]


if __name__ == "__main__":
    MRMostFrequentWord.run()
