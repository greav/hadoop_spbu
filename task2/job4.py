import re

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol

WORD_RE = re.compile(r"\w+")


class MRCapitalizedWords(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), (1, int(word[0].isupper()))

    def combiner(self, word, pairs):
        yield word, [sum(each) for each in zip(*pairs)]

    def reducer(self, word, pairs):
        total, n_capitalize = (sum(each) for each in zip(*pairs))
        if total > 10 and n_capitalize > total / 2:
            yield word, str((n_capitalize, total))


if __name__ == "__main__":
    MRCapitalizedWords.run()
