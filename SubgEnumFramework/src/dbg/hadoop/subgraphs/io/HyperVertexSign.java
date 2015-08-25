package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import dbg.hadoop.subgraphs.utils.HyperVertex;


public class HyperVertexSign implements Writable, WritableComparable {
	private long vertex;
	private int sign;
	
	public HyperVertexSign(){
		vertex = 0;
		sign = 0;
	}
	
	//public DoubleIntegerPairWritable(int[] integers){
	//	this.set(integers);
	//}
	
	public HyperVertexSign(long _vertex, int _sign){
		this.set(_vertex, _sign);
	}
	
	public HyperVertexSign(HyperVertexSign values) {
		// TODO Auto-generated constructor stub
		this.set(values.getVertex(), values.getSign());
	}

	public void set(long _vertex, int _sign){
		this.vertex = _vertex;
		this.sign = _sign;
	}
	
	public long getVertex(){
		return this.vertex;
	}
	
	public int getSign(){
		return this.sign;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.vertex = in.readLong();
		this.sign = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(this.vertex);
		out.writeInt(this.sign);
	}

	@Override
	public int compareTo(Object obj) {
		// TODO Auto-generated method stub
		int cmp = 0;
		HyperVertexSign that = (HyperVertexSign)obj;
		if(this.vertex == that.getVertex() && this.sign == that.getSign()){
			return cmp;
		}
		else if(this.vertex != that.getVertex()){
			cmp = HyperVertex.compare(this.vertex, that.getVertex());
		}
		else{
			cmp = (this.sign < that.getSign()) ? -1 : 1;
		}

		return cmp;		
	}
	
	@Override
	public String toString(){
		return HyperVertex.toString(vertex) + "," + sign;
	}
	
	@Override
	public int hashCode(){
		return (31 * HyperVertex.VertexID(vertex));
	}

}