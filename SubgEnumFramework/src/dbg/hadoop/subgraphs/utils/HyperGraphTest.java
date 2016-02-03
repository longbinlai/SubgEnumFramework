package dbg.hadoop.subgraphs.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class HyperGraphTest {
	public static void main(String[] args) throws IOException{
		if(args.length != 2){
			System.out.println("HyperGraphTest <graph_file> <delim>");
			System.exit(-1);
		}
		String graphFile = args[0];
		String delim = args[1];
		String line = "";
		BufferedReader reader = new BufferedReader(new FileReader(new File(graphFile)));
		Graph g = new Graph();
		while((line = reader.readLine()) != null){
			if(line.startsWith("#")){
				continue;
			}
			String[] s = line.split(delim);
			if(s.length != 2 || Long.valueOf(s[0]) == Long.valueOf(s[1])){
				System.err.println("Format error: " + line);
				continue;
			}
			g.addEdge(Long.valueOf(s[0]), Long.valueOf(s[1]));
		}
		reader.close();
		
		long begin = System.currentTimeMillis();
		g.genHyperGraph();
		long end = System.currentTimeMillis();
		System.out.println("Time elapsed: " + (end - begin) +  " ms");
	}
}