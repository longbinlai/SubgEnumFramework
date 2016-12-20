from briefly import *
from briefly.common import *

objs = Pipeline("subgenum hadoop pipeline")
prop = objs.prop

@simple_hadoop_process
def subgenum_frame(self):
  self.config.defaults(
    main_class = 'dbg.hadoop.subgenum.frame.MainEntry', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['enum.query=${query}', \
            'mapred.input.file=${input}', \
            'mapred.reduce.tasks=${num_reducers}', \
            'jar.file.name=${jar_file}', \
            'clique.number.vertices=${clique_size}', \
            'result.compression=${result_compression}', \
            'bloom.filter.false.positive.rate=${bf_falsepositive}', \
            'bloom.filter.type=${bf_type}', \
            'enable.bloom.filter=${bf_enable}', \
            'map.input.max.size=${max_size}', \
            'count.pattern.once=${count_pattern_once}', \
            'count.only=${count_only}', \
            'enum.clique.v2=${enum_clique_v2}', \
            'enum.house.square.partition=${square_partition}', \
            'enum.house.square.partition.thresh=${square_partition_thresh}', \
            'enum.solarsquare.chordalsquare.partition=${chordalsquare_partition}', \
            'enum.solarsquare.chordalsquare.partition.thresh=${chordalsquare_partition_thresh}', \
            'skip.chordalsquare=${skip_chordalsquare}', \
            'skip.square=${skip_square}', \
            'is.leftdeep=${left_deep}', \
            'is.nonoverlapping=${nonoverlapping}'
            ]
  )

data = '[please specify your dataset name]'
prop['input'] = data + "/dd"
prop['query'] = 'Please specify your query, supported are:'
'''
square: q1 in the paper
chordalsquare: q2
clique: (q3 and q7) If you put clique in this part, you have to also suggest the number of nodes in the clique by
putting prop['clique_size'] = 5, 6 .... Default is 4-clique
house: q4
tcsquare: q5
near5clique: q6
'''
prop['count_pattern_once'] = 'Please assign this to true if you want to get the sum results'

target = objs | subgenum_frame() 

objs.run(target)
