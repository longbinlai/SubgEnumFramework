package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class HVArraySign implements WritableComparable, Writable{
	public HVArray vertexArray;
	public int sign;
	
	public HVArraySign(){
		this.vertexArray = new HVArray();
		this.sign = Integer.MIN_VALUE;
	}
	
	public HVArraySign(long _v, int _sign){
		HVArray _array = new HVArray(_v);
		this.set(_array, _sign);
	}

	public HVArraySign(long _v1, long _v2,
			int _sign) {
		HVArray _array = new HVArray(_v1, _v2);
		this.set(_array, _sign);
	}

	public HVArraySign(long _v1, long _v2, long _v3, int _sign) {
		HVArray _array = new HVArray(_v1, _v2, _v3);
		this.set(_array, _sign);
	}
	
	public HVArraySign(HVArray _array, int _sign){
		this.set(_array, _sign);
	}

	public void set(HVArray _array, int _sign){
		this.vertexArray = _array;
		this.sign = _sign;
	}
	
	/**
	 * This is for the convenience of twintwig generation
	 * @param _v1
	 * @param _v2
	 * @param _v3
	 * @param _keyMap
	 * @param _sign
	 */
	public HVArraySign(long _v1, long _v2,
			long _v3, int _sign, byte keyMap) {
		this.set(_v1, _v2, _v3, _sign, keyMap);
	}
	

	private void set(long _v1, long _v2, long _v3, int _sign, byte _keyMap){
		this.sign = _sign;
		switch (_keyMap){
		case 0: //000
			this.vertexArray = new HVArray();
			this.sign = Integer.MIN_VALUE;
			break;		
		case 1: // 001
			this.vertexArray = new HVArray(_v3);
			break;		
		case 2: // 010
			this.vertexArray = new HVArray(_v2);
			break;
		case 3: // 011
			this.vertexArray = new HVArray(_v2, _v3);
			break;
		case 4: // 100
			this.vertexArray = new HVArray(_v1);
			break;
		case 5: // 101
			this.vertexArray = new HVArray(_v1, _v3);
			break;
		case 6: // 110
			this.vertexArray = new HVArray(_v1, _v2);
			break;
		case 7: // 111
			this.vertexArray = new HVArray(_v1, _v2, _v3);
			break;
		default:
			break;	
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.vertexArray.readFields(in);
		this.sign = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.vertexArray.write(out);
		out.writeInt(this.sign);
		
	}
	
	@Override
	public String toString(){
		return this.vertexArray + "+" + this.sign;
	}
	
	@Override
	public int hashCode(){
		return this.vertexArray.hashCode();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
}