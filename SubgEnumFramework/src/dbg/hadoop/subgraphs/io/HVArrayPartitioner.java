package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HVArrayPartitioner 
extends Partitioner<HVArray, Writable>{

	@Override
	public int getPartition(HVArray key, Writable value,
			int numPartitions) {
		// TODO Auto-generated method stub
		return ((key.hashCode() & Integer.MAX_VALUE) % numPartitions);
	}
}