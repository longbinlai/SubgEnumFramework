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
		
		long[] array = { 0, 0, 4, 1, 2, 3, 4, 6, 2, 5, 6, 1, 2, 3 };
		
		int cliqueSize = (int) array[2];
		// long[] cliqueArray = Arrays.copyOfRange(_value.toArrays(), 3, 3 +
		// cliqueSize);

		if (cliqueSize > 0) {
			long[] cliqueArray = Arrays.copyOfRange(array, 3, 3 + cliqueSize);
			handleCliqueCompress(cliqueArray);
		}
		int index = 3 + cliqueSize;
		while (index < array.length) {
			int len = (int) array[index];
			int nonCliqueVerticesSize = (int) array[index + 1];
			long[] prefix = Arrays.copyOfRange(array, index + 2, index + 2
					+ nonCliqueVerticesSize);
			long[] cliqueArray = Arrays.copyOfRange(array, index + 2
					+ nonCliqueVerticesSize, index + len + 1);
			// System.out.println("prefix: " +
			// HyperVertex.HVArrayToString(prefix));
			// System.out.println("prefix: " +
			// HyperVertex.HVArrayToString(cliqueArray));
			handleCliquePartialCompress(prefix, cliqueArray);
			index += len + 1;
		}
		
		/*
		float p = 0.1f;
		//int[] nonSetNodeSizes = { 100, 300, 500, 1000, 2000, 3000, 4000, 5000 };
		//int[] setNodeSizes = { 20, 50, 80, 100, 200, 300, 400, 500 };
		//int[] cliqueSizes = { 3, 4, 5 };
		int[] nonSetNodeSizes = { 389 };
		int[] setNodeSizes = { 0 };
		int[] cliqueSizes = { 4 };
		
		Graph g1 = new Graph();
		Graph g2 = new Graph();
		TLongHashSet set = new TLongHashSet();
		
		System.out.println("k\t#NonSetNodes\t#SetNodes\tPerformance Gain");
		for (int k : cliqueSizes) {
			for (int sizeOfNonSetNodes : nonSetNodeSizes) {
				for (int sizeOfSetNodes : setNodeSizes) {
					g1.clear();
					g2.clear();
					set.clear();
					
					int sizeTotal = sizeOfNonSetNodes + sizeOfSetNodes;
					int nodeNum = (int) (1 / p);
					Random rand = new Random(System.currentTimeMillis());

					for (long i = 0; i < sizeOfNonSetNodes - 1; ++i) {
						for (long j = i + 1; j < sizeOfNonSetNodes; ++j) {
							if (rand.nextInt() % nodeNum == 0) {
								g1.addEdge(i, j);
								g2.addEdge(i, j);
							}
						}
					}

					for (long i = sizeOfNonSetNodes; i < sizeTotal; ++i) {
						g2.addSetNodes(i);
						for (long j = i + 1; j < sizeTotal; ++j) {
							g1.addEdge(i, j);
						}
					}

					for (long i = 0; i < sizeOfNonSetNodes; ++i) {
						for (long j = sizeOfNonSetNodes; j < sizeTotal; ++j) {
							if (rand.nextInt() % nodeNum == 0) {
								g1.addEdge(i, j);
								g2.addEdge(i, j);
							}
						}
					}

					long start = System.currentTimeMillis();
					long res1 = g1.countCliquesOfSize(k);
					long end = System.currentTimeMillis();
					long time1 = end - start;
					
					start = System.currentTimeMillis();
					long[] res2 = g2.enumClique(k, 0, true);
					end = System.currentTimeMillis();
					long time2 = end - start;
					
					float performGain = time1 / (float)time2;
					
					System.out.println("Method 1: " + time1);
					System.out.println("Method 2: " + time2);
					
					if(res1 != res2[0]) {
						System.err.println("Result 1: " + res1 + " is not equal to result 2: " + res2[0]);
					}
					
					System.out.println(k + "\t" + sizeOfNonSetNodes + "\t"
							+ sizeOfSetNodes + "\t" + performGain);
				}
			}
		}
		*/
		//System.out.println(CliqueEncoder.getNumCliquesFromEncodedArrayV2(res1));
		//System.out.println(CliqueEncoder.getNumCliquesFromEncodedArrayV2(res2));
		
		//System.out.println(HyperVertex.HVArrayToString(res1));
		//System.out.println(HyperVertex.HVArrayToString(res2));
		
		/*

		int i = 2;
		int size = (int)res2[2];
		
		HashSet<String> hashSet = new HashSet<String>();
		System.out.println();
		while(i < res2.length) {
			if(i == 2) {
				long[] array = Arrays.copyOfRange(res2, 3, 3 + size);
				for(int t = 0; t < array.length - 3; ++t) {
					for(int j = t + 1; j < array.length - 2; ++j){
						for(int k = j + 1; k < array.length - 1; ++k) {
							for(int s = k + 1; s < array.length; ++s) {
								long[] clique = { array[t], array[j], array[k], array[s] };
								
								hashSet.add(HyperVertex.HVArrayToString(clique));
							}
						}
					}
				}
			}
			else {
				size = (int)res2[i];
				int nonCliqueSize = (int)res2[i + 1];
				long [] nonCliqueNodes = Arrays.copyOfRange(res2, i + 2, i + 2 + nonCliqueSize);
				int cliqueSize = size - 1 - nonCliqueSize;
				long [] cliqueNodes = Arrays.copyOfRange(res2, i + 2 + nonCliqueSize, i + size + 1);
				if(nonCliqueSize == 1) {
					for(int j = 0; j < cliqueNodes.length - 2; ++j){
						for(int k = j + 1; k < cliqueNodes.length - 1; ++k) {
							for(int s = k + 1; s < cliqueNodes.length; ++s) {
								long[] clique = { nonCliqueNodes[0],cliqueNodes[j], 
										cliqueNodes[k], cliqueNodes[s] };
								Arrays.sort(clique);
								hashSet.add(HyperVertex.HVArrayToString(clique));
							}
						}
					}
				}
				if(nonCliqueSize == 2) {
					for(int k = 0; k < cliqueNodes.length - 1; ++k) {
						for(int s = k + 1; s < cliqueNodes.length; ++s) {
							long[] clique = { nonCliqueNodes[0], nonCliqueNodes[1], 
									cliqueNodes[k], cliqueNodes[s] };
							Arrays.sort(clique);
							hashSet.add(HyperVertex.HVArrayToString(clique));
						}
					}
				}
				if (nonCliqueSize == 3) {
					for(int s = 0; s < cliqueNodes.length; ++s) {
						long[] clique = { nonCliqueNodes[0], nonCliqueNodes[1], 
								nonCliqueNodes[2] , cliqueNodes[s] };
						Arrays.sort(clique);
						hashSet.add(HyperVertex.HVArrayToString(clique));
					}
				}
				if (nonCliqueSize == 4) {
					long[] clique = { nonCliqueNodes[0], nonCliqueNodes[1], 
								nonCliqueNodes[2] , nonCliqueNodes[3] };
					Arrays.sort(clique);
					hashSet.add(HyperVertex.HVArrayToString(clique));
				}
			}
			i += size + 1;
		}
		
		for(int t = 3; t < res1.length; t += 4) {
			long[] array = { res1[t], res1[t + 1], res1[t + 2], res1[t + 3] };
			if(!hashSet.contains(HyperVertex.HVArrayToString(array))){
				System.out.println(HyperVertex.HVArrayToString(array));
			}
		}

		
		System.out.println();
		
		System.out.println(HyperVertex.HVArrayToString(res1));
		System.out.println(HyperVertex.HVArrayToString(res2));
		System.out.println(CliqueEncoder.getNumCliquesFromEncodedArrayV2(res1));
		System.out.println(CliqueEncoder.getNumCliquesFromEncodedArrayV2(res2));
		*/
		
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