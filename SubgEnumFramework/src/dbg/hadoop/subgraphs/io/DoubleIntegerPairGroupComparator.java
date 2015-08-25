package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.WritableComparator;

public class DoubleIntegerPairGroupComparator extends WritableComparator {

	protected DoubleIntegerPairGroupComparator() {
		super(DoubleIntegerPairWritable.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		int cmp = 0;
		int i1 = readInt(b1, s1);
		int i2 = readInt(b2, s2);
		cmp = (i1 < i2) ? -1 : ((i1 == i2) ? 0: 1);
		return cmp;
	}
	
}