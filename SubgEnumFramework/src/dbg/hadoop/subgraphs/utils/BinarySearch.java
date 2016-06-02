package dbg.hadoop.subgraphs.utils;

public class BinarySearch {
	public static int binarySearch(long target, long[] list) {
		int left = 0;
		int right = list.length - 1;
		while (left <= right) {
			int mid = left + ((right - left) >> 1);
			if (list[mid] == target)
				return mid;
			else if (list[mid] > target)
				right = mid - 1;
			else
				left = mid + 1;
		}
		return -1;
	}

	public static int findLargeIndex(long target, long[] list) {
		int res = -1;
		int left = 0;
		int right = list.length - 1;
		while (left <= right) {
			int mid = left + ((right - left) >> 1);
			if (list[mid] == target) {
				res = mid + 1;
				break;
			} else if (list[mid] > target) {
				right = mid - 1;
			} else {
				left = mid + 1;
			}
		}
		if(res == -1){
			res = left;
		}
		return res;
	}
}