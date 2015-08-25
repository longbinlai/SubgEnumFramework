package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.WritableComparator;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.HyperVertex;

/**
 * We simply use the hyperVertexArrayComparator by ingnoring the sign.
 * @author robeen
 *
 */
public class HVArraySignComparator extends WritableComparator {

	protected HVArraySignComparator() {
		super(HVArraySign.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		int cmp = 0;
		int len1 = readInt(b1, s1);
		int len2 = readInt(b2, s2);
		cmp = (len1 == len2)? 0 : ((len1 < len2)? -1 : 1);
		if(cmp != 0){
			return cmp;
		}
		long cpr1 = 0L, cpr2 = 0L;
		for(int i = 0; i < len1; ++i){
			cpr1 = readLong(b1, s1 + Config.NUMINTBITS + i * Config.NUMLONGBITS) & HyperVertex.COMP_MASK;
			cpr2 = readLong(b2, s2 + Config.NUMINTBITS + i * Config.NUMLONGBITS) & HyperVertex.COMP_MASK;
			cmp = (cpr1 < cpr2) ? -1 : ((cpr1 == cpr2) ? 0: 1);
			if(0 != cmp){
				return cmp;
			}
		}
		int sign1 = readInt(b1, s1 + Config.NUMINTBITS + len1 * Config.NUMLONGBITS);
		int sign2 = readInt(b2, s2 + Config.NUMINTBITS + len1 * Config.NUMLONGBITS);
		cmp = (sign1 < sign2) ? -1 : ((sign1 == sign2) ? 0: 1);
		
		return cmp; 
	}
	
}