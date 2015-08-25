package dbg.hadoop.subgraphs.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Writable;


public class MaxHeap implements Writable{
	private int length;
	private int heapSize;
	/**
	 * The arrays should be indexed with 1
	 */
	
	private int[] arrays;
	
	public MaxHeap(MaxHeap other){
		this.length = this.heapSize = other.size();
		int otherArray[] = other.toArrays();
		int l = otherArray.length;
		this.arrays = new int[l];
		for(int i = 0; i < l; ++i){
			this.arrays[i] = otherArray[i];
		}
	}
	
	public MaxHeap(int[] inputArrays){
		if (inputArrays[0] != -1) {
			length = inputArrays.length;
			this.arrays = new int[length + 1];
			this.arrays[0] = -1; // We would not use it
			for (int i = 0; i < this.length; ++i) {
				arrays[i + 1] = inputArrays[i];
			}
		}
		else{
			length = inputArrays.length - 1;;
			this.arrays = inputArrays;
		}
		heapSize = length;
	}
	
	
	public MaxHeap(int initialSize){
		init(initialSize);
	}
	
	
	/**
	 * Max heap should satisfy that A[parent[i]] >= A[i]
	 * Before call maxHeapify, we assume that the heap rooted on both left(i) and right(i)
	 * are already maxHeap.
	 * @param i The index to be adjusted to max heap
	 */
	public void maxHeapify(int i){
		int l = left(i);
		int r = right(i);
		int largest = i;
		if(l <= heapSize && Utility.compareVertex(arrays[i], arrays[l]) < 0){
			largest = l;
		}
		if(r <= heapSize && Utility.compareVertex(arrays[largest], arrays[r]) < 0){
			largest = r;
		}
		if(largest != i){
			swap(i, largest);
			maxHeapify(largest);
		}
	}
	
	public void buildMaxHeap(){
		for(int i = length / 2; i >=1; --i){
			maxHeapify(i);
		}
	}
	
	/**
	 * The sort should be proceeded after build heap
	 */
	public void sort(){
		buildMaxHeap();
		for(int i = length; i >= 2; --i){
			swap(1, i);
			heapSize -= 1;
			maxHeapify(1);
		}
		this.heapSize = this.length;
	}
	
	public void printArray(){
		System.out.print("arrays: { ");
		for(int i = 1; i <= length; ++i){
			System.out.print(arrays[i] + ",");
		}
		System.out.println("} ");
	}
	
	public static void printArray(int[] inputs){
		System.out.print("inputs: { ");
		for(int i = 0; i < inputs.length; ++i){
			System.out.print(inputs[i] + ",");
		}
		System.out.println("} ");
	}
	
	private void swap(int srcIndex, int dstIndex){
		int tmp = arrays[srcIndex];
		arrays[srcIndex] = arrays[dstIndex];
		arrays[dstIndex] = tmp;
	}
	
	private int left(int i){
		return (i << 1);
	}
	
	private int right(int i){
		return (i << 1 | 0x1);
	}
	
	private int parent(int i){
		return (i >> 1);
	}
	
	private void resize() {
		int[] newArrays = new int[arrays.length << 1];
		for (int i = 0; i < arrays.length; ++i)
			newArrays[i] = arrays[i];
		arrays = newArrays;
	}
	
	public boolean isEmpty() {
		return (this.length == 0);
	}
	
	public int size(){
		return this.length;
	}
	
	/**
	 * Insert a new element to the heap
	 * The insertion should follow the max heap constraints,
	 * also, when the insertion break the space limits,
	 * resize the array to twice larger of its previous space.
	 * @param k
	 */
	public void insert(int k) {
		if (size() == arrays.length - 1){
			resize();
		}
		arrays[length + 1] = k;
		int i = length + 1;
		while (i != 1 && Utility.compareVertex(arrays[i], arrays[parent(i)]) > 0) {
			swap(i, parent(i));
			i = parent(i);
		}
		++length;
		++heapSize;
	}
	
	public int getFirst(){
		return this.arrays[1];
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		this.length = in.readInt();
		this.heapSize = this.length;
		this.arrays = new int[this.length + 1];
		this.arrays[0] = -1; // For consistent
		for(int i = 1; i <= length; ++i){
			this.arrays[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.length);
		for(int i = 1; i <= this.length; ++i){
			out.writeInt(arrays[i]);
		}
	}
	
	@Override
	public String toString(){
		String res = "";
		if(this.isEmpty()){
			return res;
		}
		for(int i = 1; i <= length; ++i){
			res += (arrays[i] + ",");
		}
		return res.substring(0, res.length() - 1);
	}
	
	public int[] toArrays(){
		return this.arrays;
	}
	
	/**
	 * Get partianl array from @from(include) to @to(exclude)
	 * @param from
	 * @param to
	 * @return
	 */
	public int[] getPartialArrays(int from, int to){
		int length = to - from;
		if(length <= 0 || from < 0 || to > this.length + 1){
			return new int[0];
		}
		else{
			int[] newArray = new int[length];
			System.arraycopy(arrays, from, newArray, 0, length);
			return newArray;
		}
	}
	
	public void init(int initialSize){
		this.arrays = new int[initialSize + 1];
		arrays[0] = -1;
		this.length = 0;
		this.heapSize = 0;
	}
	
	public int binarySearch(int key){
		return Arrays.binarySearch(this.arrays, key);
	}
	
	public void clear(){
		this.arrays = null;
	}
}