package dbg.hadoop.subgraphs.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.utils.HyperVertexHeap;

/**
 * @author robeen
 *
 */
public class HyperVertexAdjList implements WritableComparable, Writable{
	private boolean firstAdd;
	private long[] smallDegreeVerticesGroup0; // This is a backup degree
	private long[] smallDegreeVerticesGroup1;
	private long[] largeDegreeVertices;
	private int sizeL;
	private int sizeS;
	
	public HyperVertexAdjList(){
		this.firstAdd = true;
		this.smallDegreeVerticesGroup0 = new long[0];
		this.smallDegreeVerticesGroup1 = new long[0];
		this.largeDegreeVertices = new long[0];
		this.sizeL = 0;
		this.sizeS = 0;
	}
	
	public HyperVertexAdjList(HyperVertexHeap large) throws Exception{	
		this.smallDegreeVerticesGroup0 = new long[0];
		this.smallDegreeVerticesGroup1 = new long[0];
		this.largeDegreeVertices = large.toArrays();
		this.sizeL = this.largeDegreeVertices.length;
		this.sizeS = 0;
	}
	
	public HyperVertexAdjList(long[] smallGroup1, HyperVertexHeap large, boolean _firstAdd){	
		this.smallDegreeVerticesGroup0 = new long[0];
		this.smallDegreeVerticesGroup1 = smallGroup1;
		this.largeDegreeVertices = large.toArrays();
		this.firstAdd = _firstAdd;
		this.sizeL = this.largeDegreeVertices.length;
		this.sizeS = this.smallDegreeVerticesGroup1.length;

	}
	
	public HyperVertexAdjList(long[] smallGroup0, long[] smallGroup1) throws Exception{
		this.smallDegreeVerticesGroup0 = smallGroup0;
		this.smallDegreeVerticesGroup1 = smallGroup1;
		//this.largeDegreeVertices = large.getPartialArrays(1, large.size() + 1);
		this.largeDegreeVertices = new long[0];
		this.sizeL = 0;
		this.sizeS = this.smallDegreeVerticesGroup1.length;
	}
	
	public boolean isFirstAdd(){
		return this.firstAdd;
	}
	
	public boolean existBackup(){
		return (this.smallDegreeVerticesGroup0.length != 0);
	}
	
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public long[] getLargeDegreeVertices(){
		return this.largeDegreeVertices;
	}
	
	public long[] getLargeDegreeVertices(int degree, boolean isMaxDegree){
		long[] res = new long[this.largeDegreeVertices.length];
		int size = 0;
		for(long elem : this.largeDegreeVertices){
			if(isMaxDegree){
				if(HyperVertex.Degree(elem) >= degree){
					continue;
				}
			} else{
				if(HyperVertex.Degree(elem) < degree){
					continue;
				}
			}
			res[size++] = elem;
		}
		return Arrays.copyOf(res, size);
	}
	
	public long[] getSmallDegreeVerticesGroup0(){
		return this.smallDegreeVerticesGroup0;
	}
	
	public long[] getSmallDegreeVerticesGroup0(int degree, boolean isMaxDegree){
		long[] res = new long[this.smallDegreeVerticesGroup0.length];
		int size = 0;
		for(long elem : this.smallDegreeVerticesGroup0){
			if(isMaxDegree){
				if(HyperVertex.Degree(elem) >= degree){
					continue;
				}
			} else{
				if(HyperVertex.Degree(elem) < degree){
					continue;
				}
			}
			res[size++] = elem;
		}
		return Arrays.copyOf(res, size);
	}
	
	public long[] getSmallDegreeVerticesGroup1(){
		return this.smallDegreeVerticesGroup1;
	}
	
	public long[] getSmallDegreeVerticesGroup1(int degree, boolean isMaxDegree){
		long[] res = new long[this.smallDegreeVerticesGroup1.length];
		int size = 0;
		for(long elem : this.smallDegreeVerticesGroup1){
			if(isMaxDegree){
				if(HyperVertex.Degree(elem) >= degree){
					continue;
				}
			} else{
				if(HyperVertex.Degree(elem) < degree){
					continue;
				}
			}
			res[size++] = elem;
		}
		return Arrays.copyOf(res, size);
	}
	
	public long[] getNeighbors(){
		long[] neighbors = new long[this.sizeL + this.sizeS];
		System.arraycopy(this.smallDegreeVerticesGroup1, 0, neighbors, 0, this.sizeS);
		System.arraycopy(this.largeDegreeVertices, 0, neighbors, this.sizeS, this.sizeL);
		return neighbors;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.firstAdd = in.readBoolean();
		
		int len1 = in.readInt();
		this.sizeS = in.readInt();
		this.sizeL = in.readInt();		
		this.smallDegreeVerticesGroup0 = new long[len1];
		this.smallDegreeVerticesGroup1 = new long[this.sizeS];
		this.largeDegreeVertices = new long[this.sizeL];
		
		for(int i = 0; i < len1; ++i){
			this.smallDegreeVerticesGroup0[i] = in.readLong();
		}
		for(int i = 0; i < this.sizeS; ++i){
			this.smallDegreeVerticesGroup1[i] = in.readLong();
		}
		for(int i = 0; i < this.sizeL; ++i){
			this.largeDegreeVertices[i] = in.readLong();
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeBoolean(this.firstAdd);
		
		int len1 = this.smallDegreeVerticesGroup0.length;
		//int len2 = this.smallDegreeVerticesGroup1.length;
		//int len3 = this.largeDegreeVertices.length;
		out.writeInt(len1);
		out.writeInt(this.sizeS);
		out.writeInt(this.sizeL);
		
		for(int i = 0; i < len1; ++i){
			out.writeLong(this.smallDegreeVerticesGroup0[i]);
		}
		for(int i = 0; i < this.sizeS; ++i){
			out.writeLong(this.smallDegreeVerticesGroup1[i]);
		}
		for(int i = 0; i < this.sizeL; ++i){
			out.writeLong(this.largeDegreeVertices[i]);
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
				res += (HyperVertex.toString(this.smallDegreeVerticesGroup0[i]) + ",");
			}
			res = res.substring(0, res.length() - 1) + "|";
		}
		else {
			res += "|";
		}
		
		if(len2 != 0){
			for(int i = 0; i < len2; ++i){
				res += (HyperVertex.toString(this.smallDegreeVerticesGroup1[i]) + ",");
			}
			res = res.substring(0, res.length() - 1) + "|";
		}
		else {
			res += "|";
		}
		
		if(len3 != 0){
			for(int i = 0; i < len3; ++i){
				res += (HyperVertex.toString(this.largeDegreeVertices[i]) + ",");
			}
			res = res.substring(0, res.length() - 1);
		}
		return res;
	}
	
	public int getSmallNum(){
		return this.sizeS;
	}
}