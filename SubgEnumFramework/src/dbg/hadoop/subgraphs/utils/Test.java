package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import dbg.hadoop.subgraphs.io.HVArray;

public class Test{
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, IOException{
		long v1 = HyperVertex.get(1, 2);
		long v2 = HyperVertex.get(2, 3);
		long v3 = HyperVertex.get(3, 1);
		long v4 = HyperVertex.get(4, 5);
		long v5 = HyperVertex.get(5, 1);
		long v6 = HyperVertex.get(6, 3);
		HVArray e1 = new HVArray(v1, v2);
		HVArray e2 = new HVArray(v3, v4);
		HVArray e3 = new HVArray(v5, v6);
		
		MaxHeap<HVArray> heap = new MaxHeap<HVArray>(10);
		heap.insert(e1);
		heap.insert(e3);
		heap.insert(e2);
		
		heap.sort();
		System.out.println(heap.toString());
	}
}