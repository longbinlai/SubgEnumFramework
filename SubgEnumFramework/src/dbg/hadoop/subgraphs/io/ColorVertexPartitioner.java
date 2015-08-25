package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import dbg.hadoop.subgraphs.utils.HyperVertex;

public class ColorVertexPartitioner 
	extends Partitioner<LongWritable, Writable>
	implements Configurable{
	private Configuration conf;
	
	@Override
	public int getPartition(LongWritable key, Writable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		int thresh = conf.getInt("mapred.large.degree.threshold", Integer.MAX_VALUE);
		return HyperVertex.Degree(key.get()) < thresh ? HyperVertex.Color(key.get()) : 0;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration _conf) {
		// TODO Auto-generated method stub
		this.conf = _conf;
	}

}