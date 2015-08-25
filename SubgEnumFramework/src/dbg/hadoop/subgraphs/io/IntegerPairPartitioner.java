package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class IntegerPairPartitioner 
extends Partitioner<IntegerPairWritable, Writable>{

	@Override
	public int getPartition(IntegerPairWritable key, Writable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		return ((key.hashCode() & Integer.MAX_VALUE) % numPartitions);
	}


}