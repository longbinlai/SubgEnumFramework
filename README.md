# Introduction
These are the source codes of the subgraph enumeration framework, implemented for the following two publications:

1. Scalable Subgraph Enumeration in MapReduce (TwinTwig Algorithm): http://dl.acm.org/citation.cfm?id=2794368

2. Scalable distributed subgraph enumeration (SEED): http://dl.acm.org/citation.cfm?id=3021937&CFID=706417948&CFTOKEN=82897186

Please cite our work for using these codes. Thank you. 

# Prerequisite
1. These codes are implemented and tested in Hadoop 2.6.2, and it should be compatible to all Hadoop 2.0 version. 
Please download and configure Hadoop in advance: http://hadoop.apache.org/

2. The program has used the lzo compression, so the lzo lib should be correctly installed in order to compile and run the codes.
https://github.com/twitter/hadoop-lzo

3. We tend to use the briefly tools to run the program. Briefly is a Python based meta programming library
designed to manage complex workflows with various type of tasks, such as Hadoop (local, Amazon EMR, or Qubole),
Java processes, and shell commands, with a minimal and elegant Hartman pipeline syntax. Please install the tool to run:
https://github.com/bloomreach/briefly

# Compile and Install
Look into the following article to see how to compile a hadoop user code using Maven (not tested):
http://tuttlem.github.io/2014/01/30/create-a-mapreduce-job-using-java-and-maven.html

Or one can try to load the project into Eclipse and build the codes using hadoop eclupse plugin. 

# How to run
As we've mentioned, we use the briefly tools to run the program. It is fairely easy to use following the instructions:
https://github.com/bloomreach/briefly

We have prepared some files to facilitate your use. 
1. subgenum.conf: This file gives several default configurations used in the program. You may need to configure 
the hadoop.home, hadoop.bin, hadoop.jar (the directory of the jar package for this program) and the num_reducers.
 Donot change the others as they either can be reconfigured elsewhere or are only for experimental purposes. 
 
2. subgenum_pre.py: The script file is used to prepare the dataset into the correct format, and to compute the
graph storage mechanism and index the large cliques for clique compression (see SEED paper). You should overwrite
the data, prop['jar_file', prop['separator'] and prop['bf_element_size'] according to your running context. 
Note the the original data file should be in lines of edges, and each edge is in the following format:

```
src_node<separator>dst_node
```

Here the separator shall be configured in prop['separator'], and we support default (tab), comma and space. 

3. subgenum_frame.py: The self-explanary script file is used to run the SEED algorithm. 

4. subgenum_twintwig.py: The file is used to run the TwinTwig algorithm.

Note that all the above scripts shall be run as:
python <python_script_file> -p subgenum.conf [-Dparam=value]

The "-p subgenum.conf" part is used to make default configuration, while the "[-Dparam=value]" can be used
to configure the parameters at hand. It is equivallent to writing prop['param'] = 'value' in the script file.
Since then, if we want to set the input folder and set the separator to comma while runing the data prepare script, we should write:

```
python subgenum_pre.py -p subgenum.conf -Dinput=<dataset_name>/<dataset_filename> -Dseparator=comma
```
