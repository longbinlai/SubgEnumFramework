package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DoubleIntegerPairPartitioner2 
extends Partitioner<DoubleIntegerPairWritable, Writable>{

	@Override
	public int getPartition(DoubleIntegerPairWritable key, Writable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		return ((31 * key.getFirst() & Integer.MAX_VALUE) % numPartitions);
	}

}