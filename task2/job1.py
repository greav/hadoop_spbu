import re

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol

WORD_RE = re.compile(r"\w+")


class MRLongestWord(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield None, word

    def combiner(self, _, words):
        yield None, max(words, key=len)

    def reducer(self, _, words):
        word = max(words, key=len)
        yield word, str(len(word))


if __name__ == "__main__":
    MRLongestWord.run()
