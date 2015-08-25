package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.WritableComparator;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;

public class HyperVertexSignComparator extends WritableComparator {

	protected HyperVertexSignComparator() {
		super(HyperVertexSign.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		int cmp = 0;
		long v1 = readLong(b1, s1);
		long v2 = readLong(b2, s2);
		cmp = HyperVertex.compare(v1, v2);
		if(0 != cmp){
			return cmp;
		}
		
		int sign1 = readInt(b1, s1 + Config.NUMLONGBITS);
		int sign2 = readInt(b2, s2 + Config.NUMLONGBITS);
		
		cmp = (sign1 < sign2) ? -1 : ((sign1 == sign2) ? 0: 1);
		return cmp;
	}
	
}