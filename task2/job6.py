import itertools
import re

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
from mrjob.step import MRStep

WORD_RE = re.compile(r"\b(?:[а-я]\.){2,}")
THRESHOLD = 0.1


class MRAbbreviation(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word, 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield None, (word, sum(counts))

    def filter_by_threshold_reducer(self, _, word_count_pairs):
        first_word_count_pairs, second_word_count_pairs = itertools.tee(
            word_count_pairs, 2
        )
        _, max_count = max(first_word_count_pairs, key=lambda item: item[1])
        for word, count in second_word_count_pairs:
            freq = count / max_count
            if freq > THRESHOLD:
                yield word, str((freq, count, max_count))

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(reducer=self.filter_by_threshold_reducer),
        ]


if __name__ == "__main__":
    MRAbbreviation.run()
