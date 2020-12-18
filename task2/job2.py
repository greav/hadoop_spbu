import re

from mrjob.job import MRJob

WORD_RE = re.compile(r"\w+")


class MRAverageWordLength(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield len(word), 1

    def combiner(self, length, counts):
        count = sum(counts)
        yield None, (length * count, count)

    def reducer(self, _, pairs):
        lengths, counts = zip(*pairs)
        yield "Average word length:", sum(lengths) / sum(counts)


if __name__ == "__main__":
    MRAverageWordLength.run()
