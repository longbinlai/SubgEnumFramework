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
import java.util.Iterator;

import dbg.hadoop.subgraphs.io.HVArray;

public class Test{
	private static TLongHashSet cliqueSet = new TLongHashSet();
	public static void main(String[] args) 
			throws InstantiationException, IllegalAccessException, IOException{

		TLongHashSet set = new TLongHashSet();
		Graph g = new Graph();
		
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
				g.addEdge(v0, v1);
			}
			else {
				set.add(v0);
				set.add(v1);
			}
		}
		
		reader.close();
		
		g.setLocalCliqueSet(set);
		
		long[] res = g.enumClique(3, 0, true);
		System.out.println(HyperVertex.HVArrayToString(res));
	}
	
	private static void addCommonNeighbors(TLongArrayList curNeighbors, TLongHashSet commonNeighbors) {
		TLongIterator iter = curNeighbors.iterator();
		while(iter.hasNext()) {
			long v = iter.next();
			if(cliqueSet.contains(v)){
				commonNeighbors.add(v);
			}
		}
	}
}