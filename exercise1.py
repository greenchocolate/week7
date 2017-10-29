from mrjob.job import MRJob
import re

class MRCountWords(MRJob):

    def mapper(self, key, line):
        for word in re.findall('\w+',line):
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__=='__main__':
    MRCountWords.run()
