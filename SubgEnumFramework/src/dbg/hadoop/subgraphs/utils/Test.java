package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;

import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.io.HVArraySign;

public class Test{
	
	private static TLongHashSet cliqueSet = new TLongHashSet();
	
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, IOException{
		Graph g = new Graph();
		g.addEdge(1, 4);
		g.addEdge(1, 2);
		g.addEdge(1, 3);
		g.addEdge(2, 4);
		g.addEdge(2, 3);
		g.addEdge(3, 4);
		g.addEdge(4, 8);
		g.addEdge(5, 8);
		g.addEdge(5, 6);
		g.addEdge(6, 7);
		g.addEdge(7, 8);
		
		g.genHyperGraph();
		
	}
	
	private static void handleCliqueCompress(long[] array) {
			TLongLinkedList list = new TLongLinkedList();
			list.addAll(array);
			//long v0 = key.get();
			long v0 = 0;
			long v1 = 0L;
			//System.out.println("key:" + HyperVertex.toString(key.get()));
			//System.out.println("cliqueMap:" + HyperVertex.toString(cliqueMap.get(key.get())));
			//if (HyperVertex.VertexID(cliqueMap.get(key.get())) 
			//		== HyperVertex.VertexID(key.get())) {
				for (int i = 0; i < array.length; ++i) {
					v1 = array[i];
					list.removeAt(i);
					System.out.println(HyperVertex.toString(v0) + "," + HyperVertex.toString(v1) 
							+ ": " + HyperVertex.HVArrayToString(list.toArray()));
					list.insert(i, v1);
				}
				list.insert(0, v0);
				//list.insert(0, -1);
				for (int i = 0; i < array.length - 1; ++i) {
					for (int j = i + 1; j < array.length; ++j) {
						// Remove i, j
						v0 = array[i];
						v1 = array[j];
						list.removeAt(i + 1);
						list.removeAt(j);
						//context.write(new HVArraySign(v0, v1, Config.LARGESIGN),
						//		new HVArray(list.toArray()));
						// Add back i, j
						System.out.println(HyperVertex.toString(v0) + "," + HyperVertex.toString(v1) 
								+ ": " + HyperVertex.HVArrayToString(list.toArray()));
						list.insert(i + 1, v0);
						list.insert(j + 1, v1);
					}
				}
			//}
		}
		
		private static void handleCliquePartialCompress(long[] prefix, long[] cliqueArray){
			int k = 3;
			int n = cliqueArray.length;
			int l = k - prefix.length;
			if(l == 0) { // Which mean the prefix with the key form a 4 -clique already
				//handleCliqueNorm(key, prefix, context);
				System.out.println("prefix only: " + HyperVertex.HVArrayToString(prefix));
				return;
			}
			long[] curClique = new long[k];
			for(int i = 0; i < prefix.length; ++i) {
				curClique[i] = prefix[i];
			}
			// Enumerating l vertices out of n in cliqueArray
			BigInteger two = BigInteger.valueOf(2);
			BigInteger start = two.pow(l).subtract(BigInteger.ONE);
			BigInteger end = BigInteger.ONE;
			for(int i = 1; i <= l; ++i) {
				end = end.add(two.pow(n - i));
			}
			
			BigInteger x = start;
			BigInteger u = BigInteger.ZERO;
			BigInteger v = BigInteger.ZERO;
			BigInteger y = BigInteger.ZERO;
			BigInteger tmp = x;
			int count = prefix.length;
			
			long[] tmpClique = new long[3];
			while(y.compareTo(end) < 0) {
				count = prefix.length;
				while(tmp.compareTo(BigInteger.ZERO) != 0) {
					int index = tmp.getLowestSetBit();
					tmp = tmp.flipBit(index);
					assert(count < k);
					curClique[count++] = cliqueArray[index];
					if(count == k) {
						System.arraycopy(curClique, 0, tmpClique, 0, k);
						Arrays.sort(tmpClique);
						//handleCliqueNorm(key, curClique, context);
						System.out.println(HyperVertex.HVArrayToString(tmpClique));
					}
				}
				
				u = x.and(x.negate()); // u = x & (-x)
				v = x.add(u);  // v = x + u
				y = v.add(v.xor(x).divide(u).shiftRight(2)); // y = v + (((v^x) / u) >> 2)
				x = y;
				tmp = x;
			}
		}
	
}