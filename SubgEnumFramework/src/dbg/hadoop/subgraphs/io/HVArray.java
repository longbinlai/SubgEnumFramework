package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import dbg.hadoop.subgraphs.utils.HyperVertex;


public class HVArray implements WritableComparable, Writable {
	protected long[] hyperVertexArray;
	protected int length = 0;
	protected int pos = 0;
	
	public HVArray(){
		this.hyperVertexArray = new long[0];
		this.length = 0;
	}
	
	public HVArray(HVArray that){
		this.set(that.toArrays());
	}
	
	public HVArray(int _size){
		this.length = _size;
		this.hyperVertexArray = new long[this.length];
	}
	
	public HVArray(long v){
		this.set(v);
	}
	
	public HVArray(long v1, long v2){
		this.set(v1, v2);
	}
	
	
	public HVArray(long v1, long v2, long v3){
		this.set(v1, v2, v3);
	}
	
	public HVArray(long[] array){
		this.set(array);
	}
	
	public HVArray(long[] smallerThanCur, long cur, long[] largerThanCur){
		int len1 = smallerThanCur.length;
		int len2 = largerThanCur.length;
		this.length = len1 + len2 + 1;
		this.hyperVertexArray = new long[this.length];
		System.arraycopy(smallerThanCur, 0, this.hyperVertexArray, 0, len1);
		this.hyperVertexArray[len1] = cur;
		System.arraycopy(largerThanCur, 0, this.hyperVertexArray, len1 + 1, len2);
	}
	
	/*
	public HVArray(long[] smallerThanCur, long[] largerThanCur){
		int len1 = smallerThanCur.length;
		int len2 = largerThanCur.length;
		this.length = len1 + len2;
		this.hyperVertexArray = new long[this.length];
		System.arraycopy(smallerThanCur, 0, this.hyperVertexArray, 0, len1);
		System.arraycopy(largerThanCur, 0, this.hyperVertexArray, len1, len2);
	}*/
	public HVArray(long[] array1, long[] array2){
		int len1 = array1.length;
		if(array2 == null){
			this.length = len1 + 1;
			this.hyperVertexArray = new long[len1 + 1];
			this.hyperVertexArray[0] = -1L;
			System.arraycopy(array1, 0, this.hyperVertexArray, 1, len1);
		}
		else{
			int len2 = array2.length;
			this.length = len1 + len2 + 1;
			this.hyperVertexArray = new long[len1 + len2 + 1];
			this.hyperVertexArray[0] = len1;
			System.arraycopy(array1, 0, this.hyperVertexArray, 1, len1);
			System.arraycopy(array2, 0, this.hyperVertexArray, len1 + 1, len2);
		}
	}
	
	/**
	 * The setting is complementary to the _keyMap.
	 * If _keyMap 001, that means we set v3 in the output key. <br>
	 * Since then, as a value, we should set v1, v2 in this case.
	 * @param _v1
	 * @param _v2
	 * @param _v3
	 * @param _keyMap
	 */
	public HVArray(long v1, long v2, long v3, byte keyMap){
		this.set(v1, v2, v3, keyMap);
	}
	
	private void set(long _v1, long _v2, long _v3, byte _keyMap){
		switch (_keyMap){
		case 0: //000
			this.set(_v1, _v2, _v3);
			break;		
		case 1: // 001
			this.set(_v1, _v2);
			break;		
		case 2: // 010
			this.set(_v1, _v3);
			break;
		case 3: // 011
			this.set(_v1);
			break;
		case 4: // 100
			this.set(_v2, _v3);
			break;
		case 5: // 101
			this.set(_v2);
			break;
		case 6: // 110
			this.set(_v3);
			break;
		case 7: // 111
			this.length = 0;
			this.hyperVertexArray = new long[0];
			break;
		default:
			break;	
		}
	}
	
	private void set(long _v){
		this.length = 1;
		this.hyperVertexArray = new long[1];
		this.hyperVertexArray[0] = _v;
	}
	
	private void set(long _v1, long _v2){
		this.length = 2;
		this.hyperVertexArray = new long[2];
		this.hyperVertexArray[0] = _v1;
		this.hyperVertexArray[1] = _v2;
	}
	
	private void set(long _v1, long _v2, long _v3){
		this.length = 3;
		this.hyperVertexArray = new long[3];
		this.hyperVertexArray[0] = _v1;
		this.hyperVertexArray[1] = _v2;
		this.hyperVertexArray[2] = _v3;
	}
	
	
	public void set(long[] vertices){
		this.length = vertices.length;
		this.hyperVertexArray = new long[this.length];
		System.arraycopy(vertices, 0, this.hyperVertexArray, 0, this.length);
	}
	
	public int size(){
		return this.length;
	}
	
	public long[] toArrays(){
		return this.hyperVertexArray;
	}
	
	public void addVertex(long v) throws Exception{
		if(pos == this.length){
			throw new Exception("pos = " + pos + " is overflowed!");
		}
		this.hyperVertexArray[pos++] = v;
	}
	
	/**
	 * Return the integer in pos
	 * @param pos
	 * @return
	 */
	public long get(int pos){
		return this.hyperVertexArray[pos];
	}
	
	public long getLast(){
		return this.hyperVertexArray[this.length - 1];
	}
	
	public long getFirst(){
		return this.hyperVertexArray[0];
	}
	
	public long getSecond(){

		return this.hyperVertexArray[1];
	}
	
	@Override
	public String toString(){
		String res = "";
		for(int i = 0; i < this.length; ++i){
			res += HyperVertex.toString(this.hyperVertexArray[i]);
			if(i != this.length - 1){
				res += ",";
			}
		}
		return res;
	}
	
	@Override
	public boolean equals(Object o){
		boolean res = true;
		if(o instanceof HVArray){
			HVArray _other = (HVArray)o;
			if(this.length != _other.size()){
				return false;
			}
			for(int i = 0; i < this.length; ++i){
				if(this.hyperVertexArray[i] != _other.get(i)){
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
		if(o instanceof HVArray){
			HVArray _other = (HVArray)o;
			for(int i = 0; i < this.length; ++i){
				if(HyperVertex.compare(hyperVertexArray[i], _other.get(i)) < 0){
					res = -1;
					break;
				}
				else if(HyperVertex.compare(hyperVertexArray[i], _other.get(i)) > 0){
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
		this.hyperVertexArray = new long[this.length];
		for(int i = 0; i < this.length; ++i){
			hyperVertexArray[i]= in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.length);
		for(int i = 0; i < this.length; ++i)
			out.writeLong(this.hyperVertexArray[i]);
	}
	
	@Override
	public int hashCode(){
		int sum = 0;
		for(int i = 0; i < this.length; ++i){
			sum += 31^(this.length - 1 - i) * this.hyperVertexArray[i];
		}
		return sum;
	}
}