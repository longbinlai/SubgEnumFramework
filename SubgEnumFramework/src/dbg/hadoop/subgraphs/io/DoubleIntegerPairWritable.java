package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class DoubleIntegerPairWritable implements Writable, WritableComparable {
	private int integer1;
	private int integer2;
	
	public DoubleIntegerPairWritable(){
		integer1 = 0;
		integer2 = 0;
	}
	
	//public DoubleIntegerPairWritable(int[] integers){
	//	this.set(integers);
	//}
	
	public DoubleIntegerPairWritable(int input1, int input2){
		this.set(input1, input2);
	}
	
	public DoubleIntegerPairWritable(DoubleIntegerPairWritable values) {
		// TODO Auto-generated constructor stub
		this.set(values.getFirst(), values.getSecond());
	}

	public void set(int input1, int input2){
		this.integer1 = input1;
		this.integer2 = input2;
	}
	
	public int getFirst(){
		return this.integer1;
	}
	
	public int getSecond(){
		return this.integer2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.integer1 = in.readInt();
		this.integer2 = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.integer1);
		out.writeInt(this.integer2);
	}

	@Override
	public int compareTo(Object obj) {
		// TODO Auto-generated method stub
		int cmp = 0;
		DoubleIntegerPairWritable dw = (DoubleIntegerPairWritable)obj;
		if(this.integer1 == dw.getFirst() & this.integer2 == dw.getSecond()){
			return cmp;
		}
		else if(this.integer1 != dw.getFirst()){
			cmp = (this.integer1 < dw.getFirst()) ? -1 : 1;
		}
		else{
			cmp = (this.integer2 < dw.getSecond()) ? -1 : 1;
		}

		return cmp;		
	}
	
	@Override
	public String toString(){
		return integer1 + "," + integer2;
	}
	
	@Override
	public int hashCode(){
		return (31 * integer1 + integer2);
	}

}