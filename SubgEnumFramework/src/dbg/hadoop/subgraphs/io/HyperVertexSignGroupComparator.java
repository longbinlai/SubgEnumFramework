package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.WritableComparator;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;

public class HyperVertexSignGroupComparator extends WritableComparator {

	protected HyperVertexSignGroupComparator() {
		super(HyperVertexSign.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		long v1 = readLong(b1, s1);
		long v2 = readLong(b2, s2);
		return HyperVertex.compare(v1, v2);
	}
	
}