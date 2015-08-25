package dbg.hadoop.subgraphs.io;

import org.apache.hadoop.io.WritableComparator;

public class IntegerPairComparator extends WritableComparator {

	protected IntegerPairComparator() {
		super(IntegerPairWritable.class);
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int cmp = 0;
		int value1 = readInt(b1, s1);
		int value2 = readInt(b2, s2); // The first int is the sizes
		cmp = (value1 == value2)? 0 : ((value1 < value2)? -1 : 1);
		if(cmp != 0){
			return cmp;
		}
		int size = value1;
		for(int i = 1; i <= size; ++i){
			value1 = readInt(b1, s1 + i * Integer.SIZE / Byte.SIZE);
			value2 = readInt(b2, s2 + i * Integer.SIZE / Byte.SIZE);
			cmp = (value1 < value2) ? -1 : ((value1 == value2) ? 0: 1);
			if(0 != cmp){
				return cmp;
			}
		}
		return cmp;
	}
	
}