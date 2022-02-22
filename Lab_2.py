from mrjob.job import MRJob
from mrjob.step import MRStep
import re

#data = open("adult.data", "r")
#print(data.read())


DATA_RE = re.compile(r"[\w.-]+")


class MRProb2_3(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_gender,
                   reducer=self.reducer_get_countgender)
        ]

    def mapper_get_gender(self, _, line):
        # yield each Female and Male
        data = DATA_RE.findall(line)
        if "Female" in data:
            Fem = 1
            yield ("Female", Fem)
        if "Male" in data:
            Male = 1
            yield ("Male", Male)

    def reducer_get_countgender(self, key, values):
        # sum all the Female and Male
        yield key, sum(values)

if __name__ == '__main__':
    MRProb2_3.run()
