package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//import dbg.hadoop.subgraphs.utils.MaxHeap;


public class IntegerPairWritable implements WritableComparable, Writable{
	protected int[] integerPair;
	protected int length = 0;
	protected int pos = 0;
	
	public IntegerPairWritable(){
	}
	
	public IntegerPairWritable(long size){
		this.length = (int)size;
		this.integerPair = new int[this.length];
	}
	
	public IntegerPairWritable(int input){
		this.length = 1;
		this.integerPair = new int[1];
		this.integerPair[0] = input;
	}
	
	public IntegerPairWritable(int input1, int input2){
		int pairs[] = { input1, input2 };
		this.set(pairs);
	}
	
	public IntegerPairWritable(IntegerPairWritable another){
		this.set(another.getIntegers());
	}
	
	public IntegerPairWritable(int[] array){
		this.set(array);
	}
	
	public IntegerPairWritable(int sign, int[] array){
		this.length = array.length + 1;
		this.integerPair = new int[this.length];
		this.integerPair[0] = sign;
		System.arraycopy(array, 0, integerPair, 1, this.length - 1);
	}
	
	public IntegerPairWritable(int sign, int[] array1, int[] array2){
		int len1 = array1.length;
		int len2 = array2.length;
		this.length = len1 + len2 + 1;
		this.integerPair = new int[this.length];
		this.integerPair[0] = sign;
		System.arraycopy(array1, 0, this.integerPair, 1, len1);
		System.arraycopy(array2, 0, this.integerPair, len1 + 1, len2);
	}
	
	/**
	 * Try to make the array a sorted array. We read two arrays and one num, <br>
	 * which satisfy that all number in smallerArray are smaller than cur and <br>
	 * all number in largeArray are larger than cur.
	 * @param smallerArray
	 * @param cur
	 * @param largerArray
	 */
	public IntegerPairWritable(int[] smallerArray, int cur, int[] largerArray){
		int len1 = smallerArray.length;
		int len2 = largerArray.length;
		this.length = len1 + len2 + 1;
		this.integerPair = new int[length];
		System.arraycopy(smallerArray, 0, this.integerPair, 0, len1);
		this.integerPair[len1] = cur;
		System.arraycopy(largerArray, 0, this.integerPair, len1 + 1, len2);
	}
	
	public IntegerPairWritable(int sign, int vertex, int[] array){
		this.length = array.length + 2;
		this.integerPair = new int[length];
		this.integerPair[0] = sign;
		this.integerPair[1] = vertex;
		System.arraycopy(array, 0, integerPair, 2, this.length - 2);
	}
	
	public IntegerPairWritable(int[] array1, int[] array2){
		int size1 = array1.length;
		int size2 = array2.length;
		this.length = size1 + size2 + 1;
		this.integerPair = new int[this.length];
		this.integerPair[0] = size1;
		System.arraycopy(array1, 0, this.integerPair, 1, size1);
		System.arraycopy(array2, 0, this.integerPair, 1 + size1, size2);
	}
	
	public IntegerPairWritable(int[] array1, int[] array2, boolean isSize){
		int size1 = array1.length;
		int size2 = array2.length;
		if(isSize){	
			this.length = size1 + size2 + 1;
			this.integerPair = new int[this.length];
			this.integerPair[0] = size1;
			System.arraycopy(array1, 0, this.integerPair, 1, size1);
			System.arraycopy(array2, 0, this.integerPair, 1 + size1, size2);
		}
		else{
			this.length = size1 + size2;
			this.integerPair = new int[this.length];
			System.arraycopy(array1, 0, this.integerPair, 0, size1);
			System.arraycopy(array2, 0, this.integerPair, size1, size2);
		}
		
	}
	
	public IntegerPairWritable(IntegerPairWritable another, int more){
		this.setLength(another.getLength() + 1);
		for(int i = 0; i < this.length - 1; ++i){
			this.integerPair[i] = another.get(i);
		}
		this.integerPair[this.length - 1] = more;
	}
	
	public IntegerPairWritable(List<Integer> list){
		this.length = list.size();
		this.integerPair = new int[this.length];
		for(int i = 0; i < this.length; ++i){
			this.integerPair[i] = list.get(i);
		}
	}
	
	public void set(int[] integerList){
		if(this.length != integerList.length){
			this.length = integerList.length;
			this.integerPair = new int[this.length];
		}
		for(int i = 0; i < this.length; ++i){
			this.integerPair[i] = integerList[i];
		}
	}
	
	public void setLength(int l){
		this.length = l;
		this.integerPair = new int[l];
	}
	
	public int getLength(){
		return this.length;
	}
	
	public int size(){
		return this.length;
	}
	
	public int[] getIntegers(){
		return this.integerPair;
	}
	
	public int getNext(){
		if (this.pos == this.length){
			this.pos = 0;
			return -1;
		}
		else{
			return this.integerPair[pos++];
		}
	}
	
	public void resetPos(){
		pos = 0;
	}
	
	public int getPos(){
		return pos;
	}
	
	public void addInteger(int input){
		this.integerPair[pos++] = input;
		if(pos == this.length){
			pos = 0;
		}
	}
	
	/**
	 * Return the integer in pos
	 * @param pos
	 * @return
	 */
	public int get(int pos){
		int res = -1;
		if(pos >= 0 & pos < this.length){
			res = this.integerPair[pos];
		}
		return res;
	}
	
	public int getLast(){
		return this.integerPair[this.length - 1];
	}
	
	public int getFirst(){
		return this.integerPair[0];
	}
	
	public int getSecond(){
		return this.integerPair[1];
	}
	
	@Override
	public String toString(){
		String res = "";
		for(int i = 0; i < this.length; ++i){
			res += String.valueOf(this.integerPair[i]);
			if(i != this.length - 1){
				res += ",";
			}
		}
		return res;
	}
	
	@Override
	public boolean equals(Object o){
		boolean res = true;
		if(o instanceof IntegerPairWritable){
			IntegerPairWritable toCompare = (IntegerPairWritable)o;
			if(this.length != toCompare.getLength()){
				return false;
			}
			for(int i = 0; i < this.length; ++i){
				if(this.integerPair[i] != toCompare.get(i)){
					res = false;
					break;
				}
			}
		}
		return res;
	}
	
	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		int res = 0;
		if(o instanceof IntegerPairWritable){
			IntegerPairWritable toCompare = (IntegerPairWritable)o;
			for(int i = 0; i < this.length; ++i){
				if(this.integerPair[i] < toCompare.get(i)){
					res = -1;
					break;
				}
				else if(this.integerPair[i] > toCompare.get(i)){
					res = 1;
					break;
				}
			}
		}
		return res;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.length = in.readInt();
		this.pos = 0;
		this.integerPair = new int[this.length];
		for(int i = 0; i < this.length; ++i){
			integerPair[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.length);
		for(int i = 0; i < this.length; ++i)
			out.writeInt(this.integerPair[i]);
	}
	
	@Override
	public int hashCode(){
		int sum = 0;
		for(int i = 0; i < this.length; ++i){
			sum += 31^i * this.integerPair[i];
		}
		return sum;
	}
	
}