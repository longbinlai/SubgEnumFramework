package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import dbg.hadoop.subgraphs.utils.MaxHeap;

/**
 * @author robeen
 *
 */
public class AdjListLimitSizeWritable implements WritableComparable, Writable{
	
	private int[] smallDegreeVerticesGroup0; // This is a backup degree
	private int[] smallDegreeVerticesGroup1;
	private int[] largeDegreeVertices;
	
	public AdjListLimitSizeWritable(){
		this.smallDegreeVerticesGroup0 = new int[0];
		this.smallDegreeVerticesGroup1 = new int[0];
		this.largeDegreeVertices = new int[0];
	}
	
	public AdjListLimitSizeWritable(MaxHeap large) throws Exception{	
		this.smallDegreeVerticesGroup0 = new int[0];
		this.smallDegreeVerticesGroup1 = new int[0];
		this.largeDegreeVertices = large.getPartialArrays(0, large.size() + 1);
	}
	
	public AdjListLimitSizeWritable(int[] smallGroup1, MaxHeap large, boolean firstAdd){	
		this.smallDegreeVerticesGroup0 = new int[0];
		this.smallDegreeVerticesGroup1 = smallGroup1;
		if(firstAdd){
			this.largeDegreeVertices = large.getPartialArrays(0, large.size() + 1);
			//this.largeDegreeVertices = large.toArrays();
		}
		else{
			this.largeDegreeVertices = large.getPartialArrays(1, large.size() + 1);
		}

	}
	
	public AdjListLimitSizeWritable(int[] smallGroup0, int[] smallGroup1) throws Exception{
		this.smallDegreeVerticesGroup0 = smallGroup0;
		this.smallDegreeVerticesGroup1 = smallGroup1;
		//this.largeDegreeVertices = large.getPartialArrays(1, large.size() + 1);
		this.largeDegreeVertices = new int[0];
	}
	
	public boolean existBackup(){
		return (this.smallDegreeVerticesGroup0.length != 0);
	}
	
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int[] getLargeDegreeVertices(){
		return this.largeDegreeVertices;
	}
	
	/**
	 * Get rid of the sign (-1) at the beginning of largeDegreeVertices 
	 * @return
	 */
	public int[] getLargeDegreeVerticesNoSign(){
		int len = this.largeDegreeVertices.length;
		int[] res = new int[len - 1];
		System.arraycopy(this.largeDegreeVertices, 1, res, 0, len - 1);
		return res;
	}
	
	public int[] getSmallDegreeVerticesGroup0(){
		return this.smallDegreeVerticesGroup0;
	}
	
	public int[] getSmallDegreeVerticesGroup1(){
		return this.smallDegreeVerticesGroup1;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len1 = in.readInt();
		int len2 = in.readInt();
		int len3 = in.readInt();
		
		this.smallDegreeVerticesGroup0 = new int[len1];
		this.smallDegreeVerticesGroup1 = new int[len2];
		this.largeDegreeVertices = new int[len3];
		
		for(int i = 0; i < len1; ++i){
			this.smallDegreeVerticesGroup0[i] = in.readInt();
		}
		for(int i = 0; i < len2; ++i){
			this.smallDegreeVerticesGroup1[i] = in.readInt();
		}
		for(int i = 0; i < len3; ++i){
			this.largeDegreeVertices[i] = in.readInt();
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int len1 = this.smallDegreeVerticesGroup0.length;
		int len2 = this.smallDegreeVerticesGroup1.length;
		int len3 = this.largeDegreeVertices.length;
		out.writeInt(len1);
		out.writeInt(len2);
		out.writeInt(len3);
		for(int i = 0; i < len1; ++i){
			out.writeInt(this.smallDegreeVerticesGroup0[i]);
		}
		for(int i = 0; i < len2; ++i){
			out.writeInt(this.smallDegreeVerticesGroup1[i]);
		}
		for(int i = 0; i < len3; ++i){
			out.writeInt(this.largeDegreeVertices[i]);
		}
	}
	
	@Override
	public String toString() {
		String res = "";
		int len1 = this.smallDegreeVerticesGroup0.length;
		int len2 = this.smallDegreeVerticesGroup1.length;
		int len3 = this.largeDegreeVertices.length;
		if(len1 != 0){
			for(int i = 0; i < len1; ++i){
				res += (this.smallDegreeVerticesGroup0[i] + ",");
			}
			res = res.substring(0, res.length() - 1) + "|";
		}
		else {
			res += "|";
		}
		
		if(len2 != 0){
			for(int i = 0; i < len2; ++i){
				res += (this.smallDegreeVerticesGroup1[i] + ",");
			}
			res = res.substring(0, res.length() - 1) + "|";
		}
		else {
			res += "|";
		}
		
		if(len3 != 0){
			for(int i = 0; i < len3; ++i){
				res += (this.largeDegreeVertices[i] + ",");
			}
			res = res.substring(0, res.length() - 1);
		}
		return res;
	}
}