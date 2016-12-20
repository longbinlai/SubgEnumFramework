from briefly import *
from briefly.common import *

objs = Pipeline("subgenum hadoop pipeline")
prop = objs.prop

@simple_hadoop_process
def preprocess(self):
  self.config.hadoop.jar = 'SubgEnumFramework_v0.2.jar' # path to your local jar
  self.config.defaults(
    main_class = 'dbg.hadoop.subgenum.prepare.PrepareData', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['mapred.input.file=${input}', \
            'mapred.reduce.tasks=${num_reducers}', \
            'jar.file.name=${jar_file}', \
            'graph.undirected=${is_undirected}', \
            'mapred.input.key.value.separator=${separator}', \
            'map.input.max.size=${max_size}', \
            'bloom.filter.element.size=${bf_element_size}', \
            'bloom.filter.false.positive.rate=${bf_falsepositive}', \
            'mapred.clique.size.threshold=${clique_size_thresh}']
  )

@simple_hadoop_process
def subgenum_frame(self):
  self.config.hadoop.jar = 'SubgEnumFramework_v0.2.jar' # path to your local jar
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
            'skip.square=${skip_square}'
            ]
  )

@simple_hadoop_process
def subgenum_twintwig(self):
  self.config.hadoop.jar = 'SubgEnumFramework_v0.2.jar' # path to your local jar
  self.config.defaults(
    main_class = 'dbg.hadoop.subgenum.twintwig.MainEntry', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['enum.query=${query}', \
            'mapred.input.file=${input}', \
            'mapred.reduce.tasks=${num_reducers}', \
            'jar.file.name=${jar_file}', \
            'clique.number.vertices=${clique_size}', \
            'bloom.filter.false.positive.rate=${bf_falsepositive}', \
            'bloom.filter.type=${bf_type}', \
            'enable.bloom.filter=${bf_enable}', \
            'count.only=${count_only}', \
            'map.input.max.size=${max_size}', \
            ]
  )

target = objs | subgenum_twintwig() 

objs.run(target)
