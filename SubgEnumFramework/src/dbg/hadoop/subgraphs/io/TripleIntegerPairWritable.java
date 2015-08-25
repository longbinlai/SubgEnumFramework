package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class TripleIntegerPairWritable implements Writable, WritableComparable{
	private int integer1 = 0;
	private int integer2 = 0;
	private int integer3 = 0;
	
	public TripleIntegerPairWritable(){
		this.set(0, 0, 0);
	}
	
	public TripleIntegerPairWritable(int[] integers){
		this.set(integers[0], integers[1], integers[2]);
	}
	
	public TripleIntegerPairWritable(IntWritable one, DoubleIntegerPairWritable two){
		//int[] integers = { one.get(), two.getFirst(), two.getSecond()};
		this.set(one.get(), two.getFirst(), two.getSecond());
	}
	
	public TripleIntegerPairWritable(int one, DoubleIntegerPairWritable two){
		//int[] integers = { one.get(), two.getFirst(), two.getSecond()};
		this.set(one, two.getFirst(), two.getSecond());
	}
	
	public TripleIntegerPairWritable(DoubleIntegerPairWritable first, int second){
		this.set(first.getFirst(), first.getSecond(), second);
	}
	
	public TripleIntegerPairWritable(int in1, int in2, int in3){
		this.set(in1, in2, in3);
	}
	
	public void set(int[] integers){
		this.set(integers[0], integers[1], integers[2]);
	}
	
	public void set(int in1, int in2, int in3){
		integer1 = in1;
		integer2 = in2;
		integer3 = in3;
	}
	
	public int getIntegers(int pos){
		if(pos == 0){
			return integer1;
		}
		else if(pos == 1){
			return integer2;
		}
		else {
			return integer3;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		integer1 = in.readInt();
		integer2 = in.readInt();
		integer3 = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(integer1);
		out.writeInt(integer2);
		out.writeInt(integer3);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		int cmp = 0;
		TripleIntegerPairWritable tw = (TripleIntegerPairWritable)o;
		if(this.integer1 == tw.getIntegers(0) & this.integer2 == tw.getIntegers(1)
				& this.integer3 == tw.getIntegers(2)){
			return cmp;
		}
		else if(this.integer1 != tw.getIntegers(0)){
			cmp = (this.integer1 < tw.getIntegers(0)) ? -1 : 1;
		}
		else if(this.integer2 != tw.getIntegers(1)){
			cmp = (this.integer2 < tw.getIntegers(1)) ? -1 : 1;
		}
		else{
			cmp = (this.integer3 < tw.getIntegers(2)) ? -1 : 1;
		}
		return cmp;
	}
	
	@Override
	public String toString(){
		return integer1 + "," + integer2 + "," + integer3;
	}
	
	@Override
	public int hashCode(){
		return 163 * integer1 + 31 * integer2 + integer3;
	}

}