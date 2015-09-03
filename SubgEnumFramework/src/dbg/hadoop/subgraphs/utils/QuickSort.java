package dbg.hadoop.subgraphs.utils;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;

import java.util.Random;

@SuppressWarnings("rawtypes")
public class QuickSort {
	
	public static int MAX_LEVELS = 1000;
	public static int ARRAY_LEN = 1000;
	
	public static void main(String[] args){

	}
	
	public static <T extends Comparable> void sort(T[] array, int p, int r){
		int q = partition(array, p, r);
		if(q > p + 1)
			sort(array, p, q);
		if(r > q + 2)
			sort(array, q + 1, r);
	}
	
	/**
	 * This Non-Recursive implementation by Darel Rex Finley. <br>
	 * Refer http://alienryderflex.com/quicksort/ <br>
	 * Despite a much more complicated implementation, the interative version <br>
	 * seems not competitive to the normal recursive version.
	 * @param arr
	 * @param p
	 * @param r
	 * @return
	 */
	public static <T extends Comparable> boolean sortIterative(T[] arr,
			int p, int r) {
		T piv;
		int i = 0, L, R;
		int[] beg = new int[MAX_LEVELS];
		int[] end = new int[MAX_LEVELS];

		beg[0] = p;
		end[0] = r;
		while (i >= 0) {
			L = beg[i];
			R = end[i] - 1;
			if (L < R) {
				piv = arr[L];
				if (i == MAX_LEVELS - 1)
					return false;
				while (L < R) {
					while (arr[R].compareTo(piv) >= 0 && L < R)
						R--;
					if (L < R)
						arr[L++] = arr[R];
					while (arr[L].compareTo(piv) <= 0 && L < R)
						L++;
					if (L < R)
						arr[R--] = arr[L];
				}
				arr[L] = piv;
				beg[i + 1] = L + 1;
				end[i + 1] = end[i];
				end[i++] = L;
			} else {
				i--;
			}
		}
		return true;
	}

	/**
	 * Partition the array A[p, ...., r - 1],
	 * 
	 * @param array
	 * @param p
	 *            The floor index, included
	 * @param r
	 *            The ceil index, excluded
	 * @return The partition boundary index
	 */
	@SuppressWarnings("unchecked")
	private static<T extends Comparable> int partition(T[] array, int p, int r){
		assert(p < r && p >= 0 && r <= array.length);
		T pivot = array[r - 1];
		int j = r - 2;
		int i = p;
		for(; i < r - 1; ++i){
			if(array[i].compareTo(pivot) >= 0){
				while(array[j].compareTo(pivot) >= 0 && j > i){
					--j;
				}
 				if(i >= j) break;
				swap(array, i, j);
				--j;
			}
		}
		if(i != r - 1){
			swap(array, i, r - 1);
		}
		return i;
	}
	
	private static<T extends Comparable> void swap(T[] array, int i, int j){
		T temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
	
	public static void printArray(Integer[] array){
		if(array == null || array.length == 0){
			return;
		}
		System.out.print("{");
		for(int v : array){
			System.out.print(v + ",");
		}
		System.out.println("}");
	}
}