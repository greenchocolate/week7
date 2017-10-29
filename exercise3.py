from mrjob.job import MRJob, MRStep

class MRIsEuler(MRJob):

    def mapper_get_nodes(self, key, line):
        edge=line.split()
        yield edge[0],-1
        yield edge[1],1

    def reducer_degree_equal(self, key, values):
        yield "is equal", sum(values)==0

    def reducer_graph_euler(self, key, degboo):
        yield "Is Euler", min(degboo)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nodes,
                   reducer=self.reducer_degree_equal),
            MRStep(reducer=self.reducer_graph_euler)
        ]


if __name__=='__main__':
    MRIsEuler.run()