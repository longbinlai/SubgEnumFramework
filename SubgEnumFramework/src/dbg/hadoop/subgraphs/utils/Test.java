package dbg.hadoop.subgraphs.utils;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

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

		TLongHashSet set = new TLongHashSet();
		Graph g1 = new Graph();
		Graph g2 = new Graph();
		
		BufferedReader reader = new BufferedReader(new FileReader(new File("res")));
		
		String line = "";
		boolean addEdge = true;
		while((line = reader.readLine()) != null){
			if(line.startsWith("Edge")){
				addEdge = true;
			}
			else {
				addEdge = false;
			}
			line = line.substring(line.indexOf('['));
			System.out.println(line);
			String[] vertices = line.split("\\],\\[");
			assert(vertices.length == 2);
			String[] v0Info = vertices[0].substring(1, vertices[0].length()).split(",");
			String[] v1Info = vertices[1].substring(0, vertices[1].length() - 1).split(",");
			long v0 = HyperVertex.get(Integer.valueOf(v0Info[0]), Integer.valueOf(v0Info[1]));
			long v1 = HyperVertex.get(Integer.valueOf(v1Info[0]), Integer.valueOf(v1Info[1]));
			if(addEdge){
				g1.addEdge(v0, v1);
				g2.addEdge(v0, v1);
			}
			else {
				g1.addEdge(v0, v1);
				set.add(v0);
				set.add(v1);
			}
		}
		
		reader.close();
		
		g1.setLocalCliqueSet(new TLongHashSet());
		g2.setLocalCliqueSet(set);
		
		long start = System.currentTimeMillis();
		long[] res1 = g1.enumClique(4, 0, false);
		long end = System.currentTimeMillis();
		
		System.out.println("Method 1, Elapsed time : " +(end - start));
		
		start = System.currentTimeMillis();
		long[] res2 = g2.enumClique(4, 0, false);
		end = System.currentTimeMillis();
		
		System.out.println("Method 2, Elapsed time : " +(end - start));
		
		long[] test = { 1, 2, 3, 4 };
		findSubsets(test);
		
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