package dbg.hadoop.subgraphs.utils;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class MaxHeap <T extends Comparable> {
	private int size;
	private int heapSize;
	
	private T[] arrays;
	
	public MaxHeap(MaxHeap<T> other){
		this.size = this.heapSize = other.size();
		T otherArray[] = other.toArray();
		int l = otherArray.length;
		this.arrays = (T[])new Comparable[l];
		System.arraycopy(otherArray, 0, this.arrays, 0, l);
	}
	
	public MaxHeap(T[] array){
		this.size = this.heapSize = array.length;
		this.arrays = (T[])new Comparable[this.size];
		System.arraycopy(array, 0, this.arrays, 0, this.size);
		//this.buildMaxHeap();
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
		if(l < heapSize && arrays[i].compareTo(arrays[l]) < 0){
			largest = l;
		}
		if(r < heapSize && arrays[largest].compareTo(arrays[r]) < 0){
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
	
	
	private void swap(int srcIndex, int dstIndex){
		T tmp = arrays[srcIndex];
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
		T[] newArrays = (T[])new Comparable[arrays.length << 1];
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
	public void insert(T k) {
		if (this.size == arrays.length){
			resize();
		}
		arrays[size] = k;
		int i = size;
		while (i != 0 && arrays[i].compareTo(arrays[parent(i)]) > 0) {
			swap(i, parent(i));
			i = parent(i);
		}
		++size;
		++heapSize;
	}
	
	public T getFirst(){
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
	
	public T[] toArray(){
		T[] res = (T[])new Comparable[this.size];
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
	
	public void init(int initialSize){
		this.arrays = (T[])new Comparable[initialSize];
		this.size = 0;
		this.heapSize = 0;
	}
	
	public void clear(){
		this.arrays = null;
		this.arrays = (T[])new Comparable[Config.HEAPINITSIZE];
		this.size = 0;
		this.heapSize = 0;
	}
}