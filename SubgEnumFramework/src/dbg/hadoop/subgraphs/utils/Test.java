package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.MemoryUsage;
import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import dbg.hadoop.subgraphs.io.HVArray;

public class Test{
	
	private static TLongHashSet cliqueSet = new TLongHashSet();
	
	private static void findSubsets(long array[]) {
		int numOfSubsets = 1 << array.length;

		for (int i = 0; i < numOfSubsets; i++) {
			int pos = array.length - 1;
			int bitmask = i;

			System.out.print("{");
			while (bitmask > 0) {
				if ((bitmask & 1) == 1)
					System.out.print(array[pos] + ",");
				bitmask >>= 1;
				pos--;
			}
			System.out.print("}");
		}
	}
	
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, IOException{
		float p = 0.1f;
		//int[] nonSetNodeSizes = { 100, 300, 500, 1000, 2000, 3000, 4000, 5000 };
		//int[] setNodeSizes = { 20, 50, 80, 100, 200, 300, 400, 500 };
		//int[] cliqueSizes = { 3, 4, 5 };
		int[] nonSetNodeSizes = { 2000 };
		int[] setNodeSizes = { 50 };
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
					long[] res1 = g1.enumClique(k, 0, true);
					long end = System.currentTimeMillis();
					
					long time1 = end - start;
					start = System.currentTimeMillis();
					long[] res2 = g2.enumClique(k, 0, true);
					end = System.currentTimeMillis();
					
					long time2 = end - start;
					
					float performGain = time1 / (float)time2;
					
					System.out.println("Method 1: " + time1);
					System.out.println("Method 2: " + time2);
					
					if(res1[0] != res2[0]) {
						System.err.println("Result 1: " + res1[0] + " is not equal to result 2: " + res2[0]);
					}
					
					System.out.println(k + "\t" + sizeOfNonSetNodes + "\t"
							+ sizeOfSetNodes + "\t" + performGain);
				}
			}
		}
		
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
	
}