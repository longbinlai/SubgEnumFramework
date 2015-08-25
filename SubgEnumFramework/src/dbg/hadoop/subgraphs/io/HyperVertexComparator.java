package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;

import dbg.hadoop.subgraphs.utils.HyperVertex;

public class HyperVertexComparator extends WritableComparator {

	protected HyperVertexComparator() {
		super(LongWritable.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){

		long cpr1 = readLong(b1, s1) & HyperVertex.COMP_MASK;
		long cpr2 = readLong(b2, s2) & HyperVertex.COMP_MASK;
		
		return (cpr1 == cpr2) ? 0 : ((cpr1 < cpr2) ? -1 : 1);
	}
	
}