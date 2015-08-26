package dbg.hadoop.subgraphs.utils;

public class BinarySearch {
	public static int binarySearch(long target, long[] list) {
		int left = 0;
		int right = list.length - 1;
		while (left <= right) {
			int mid = left + (right - left) / 2;
			if (list[mid] == target)
				return mid;
			else if (HyperVertex.compare(list[mid], target) > 0)
				right = mid - 1;
			else
				left = mid + 1;
		}
		return -1;
	}

	public static int findLargeIndex(long target, long[] list) {
		int res = 0;
		int left = 0;
		int right = list.length - 1;
		while (left <= right) {
			int mid = left + (right - left) / 2;
			if (list[mid] == target) {
				res = mid + 1;
				break;
			} else if (HyperVertex.compare(list[mid], target) > 0) {
				right = mid - 1;
			} else {
				left = mid + 1;
			}
			if (left > right) {
				if (HyperVertex.compare(list[mid], target) > 0) {
					res = mid;
					break;
				} else {
					res = mid + 1;
					break;
				}
			}
		}
		return res;
	}
}