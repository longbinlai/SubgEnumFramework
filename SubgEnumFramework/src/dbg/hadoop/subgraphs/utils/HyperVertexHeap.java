package dbg.hadoop.subgraphs.utils;


public class HyperVertexHeap{
	private int size;
	private int heapSize;
	
	private long[] arrays;
	
	public static void main(String[] args) throws Exception{
		//HyperVertexHeap heap = new HyperVertexHeap(5);
	}
	
	public HyperVertexHeap(HyperVertexHeap other){
		this.size = this.heapSize = other.size();
		long otherArray[] = other.toArrays();
		int l = otherArray.length;
		this.arrays = new long[l];
		System.arraycopy(otherArray, 0, this.arrays, 0, l);
	}
	
	public HyperVertexHeap(long[] array){
		this.size = this.heapSize = array.length;
		this.arrays = new long[this.size];
		System.arraycopy(array, 0, this.arrays, 0, this.size);
		this.buildMaxHeap();
	}
	
	public HyperVertexHeap(int initialSize){
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
		if(l < heapSize && HyperVertex.compare(arrays[i], arrays[l]) < 0){
			largest = l;
		}
		if(r < heapSize && HyperVertex.compare(arrays[largest], arrays[r]) < 0){
			largest = r;
		}
		if(largest != i){
			swap(i, largest);
			maxHeapify(largest);
		}
	}
	
	public void buildMaxHeap(){
		for(int i = size / 2; i >= 0; --i){
			maxHeapify(i);
		}
	}
	
	/**
	 * The sort should be proceeded after build heap
	 */
	public void sort(){
		buildMaxHeap();
		for(int i = size - 1; i > 0; --i){
			swap(0, i);
			heapSize -= 1;
			maxHeapify(0);
		}
		this.heapSize = this.size;
	}
	
	public void printArray(){
		System.out.print("arrays: { ");
		for(int i = 1; i <= size; ++i){
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
		long tmp = arrays[srcIndex];
		arrays[srcIndex] = arrays[dstIndex];
		arrays[dstIndex] = tmp;
	}
	
	private int left(int i){
		return ((i << 1) | 1);
	}
	
	private int right(int i){
		return ((i << 1) + 2);
	}
	
	private int parent(int i){
		return ((i - 1) >> 1);
	}
	
	private void resize() {
		long[] newArrays = new long[arrays.length << 1];
		//for (int i = 0; i < arrays.length; ++i)
		//	newArrays[i] = arrays[i];
		System.arraycopy(this.arrays, 0, newArrays, 0, arrays.length);
		arrays = newArrays;
	}
	
	public boolean isEmpty() {
		return (this.size == 0);
	}
	
	public int size(){
		return this.size;
	}
	
	/**
	 * Insert a new element to the heap
	 * The insertion should follow the max heap constraints,
	 * also, when the insertion break the space limits,
	 * resize the array to twice larger of its previous space.
	 * @param k
	 */
	public void insert(long k) {
		if (size() == arrays.length){
			resize();
		}
		arrays[size] = k;
		int i = size;
		while (i != 0 && HyperVertex.compare(arrays[i], arrays[parent(i)]) > 0) {
			swap(i, parent(i));
			i = parent(i);
		}
		++size;
		++heapSize;
	}
	
	public long getFirst(){
		return this.arrays[0];
	}
	
	@Override
	public String toString(){
		String res = "";
		if(this.isEmpty()){
			return res;
		}
		for(int i = 0; i < size; ++i){
			res += (arrays[i] + ",");
		}
		return res.substring(0, res.length() - 1);
	}
	
	public long[] toArrays(){
		long[] res = new long[this.size];
		System.arraycopy(this.arrays, 0, res, 0, this.size);
		return res;
	}
	
	/**
	 * Get partianl array from @from(include) to @to(exclude)
	 * @param from
	 * @param to
	 * @return
	 */
	public long[] getPartialArrays(int from, int to){
		int len = to - from;
		if(len <= 0 || from < 0 || to > this.size){
			return new long[0];
		}
		else{
			long[] newArray = new long[len];
			System.arraycopy(arrays, from, newArray, 0, len);
			return newArray;
		}
	}
	
	/**
	 * Use binary search to find specific items.
	 * @param toFind
	 * @return
	 */
	public boolean contains(long toFind){
		boolean res = false;
		int begin = 0, end = this.size;
		int index = this.size / 2;
		while(begin != end){
			if(HyperVertex.compare(toFind, this.arrays[index]) < 0){
				end = (index == 0)? 0 : index - 1;
			}
			else if(HyperVertex.compare(toFind, this.arrays[index]) > 0){
				begin = index + 1;
			}
			else{
				res = true;
				break;
			}
			index = begin + (end - begin) / 2;
		}
		if(HyperVertex.compare(toFind, this.arrays[index]) == 0){
			res = true;
		}
		return res;
		
	}
	
	public void init(int initialSize){
		this.arrays = new long[initialSize];
		this.size = 0;
		this.heapSize = 0;
	}
	
	public void clear(){
		this.arrays = null;
	}
}