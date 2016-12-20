from briefly import *
from briefly.common import *
import os

objs = Pipeline("subgenum hadoop pipeline")
prop = objs.prop

@simple_hadoop_process
def preprocess(self):
  self.config.hadoop.jar = 'SubgEnumFramework_cloud.jar' # path to your local jar
  self.config.defaults(
    main_class = 'dbg.hadoop.subgenum.prepare.PrepareData', # This is special for hadoop-examples.jar. Use full class name instead.
    args = ['mapred.input.file=${input}', \
            'mapred.reduce.tasks=${num_reducers}', \
            'jar.file.name=${jar_file}', \
            'graph.undirected=${is_undirected}', \
            'mapred.input.key.value.separator=${separator}', \
            'map.input.max.size=${max_size}', \
            'enable.bloom.filter=${bf_enable}', \
            'bloom.filter.element.size=${bf_element_size}', \
            'bloom.filter.false.positive.rate=${bf_falsepositive}', \
            'mapred.clique.size.threshold=${clique_size_thresh}']
  )

if __name__ == '__main__':
    '''
    This is the setting of several parameters for preparing the dataset
    data: should be replaced with the dataset name. For example, if you
    name you dataset `abc`, then you should create a folder named `abc`
    under the home directory of hdfs.
    
    '''
    data = "[please_put_your_dataset_name]"
    target = objs | preprocess()
    prop['input'] = data + '/dd'
    prop['jar_file'] = '[put your jar file directory]'
    prop['separator'] = '[default[tab] | comma | space]'
    prop['bf_element_size'] = 'Number of edges in your graph data'
    objs.run(target)
    os.system("cp build/preprocess-* build/" + data + "-preprocess.log")
