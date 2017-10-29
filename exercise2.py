from mrjob.job import MRJob, MRStep

class MRIsEuler(MRJob):

    def mapper_get_nodes(self, key, line):
        for u in line.split():
            yield u, 1

    def reducer_degree_even(self, key, values):
        yield "Is even", 1-sum(values)%2      #(1-values%2) is 0 if odd and 1 if even.

    def reducer_graph_euler(self, key, degboo):
        yield "Is Euler", min(degboo)              #min is 1 iff all nodes are even

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_nodes,
                   reducer=self.reducer_degree_even),
            MRStep(reducer=self.reducer_graph_euler)
        ]


if __name__=='__main__':
    MRIsEuler.run()