package dbg.hadoop.subgenum.hypergraph.adjlist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class GenAdjList{
	private static InputInfo inputInfo = null;
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String numReducers = inputInfo.numReducers;
		String inputFile = inputInfo.inputFile;
		String jarFile = inputInfo.jarFile;
		int maxSize = inputInfo.maxSize;
		boolean isHyper = inputInfo.isHyper;
		String workDir = inputInfo.workDir;
		
		if(inputFile.isEmpty()){
			System.err.println("Input file not specified!");
			return;
		}
		
		Configuration conf = new Configuration();
		conf.setInt("map.input.max.size", maxSize);
		
		String outputDir = workDir + Config.hyperGraphAdjList + "." + maxSize;
		String[] opts = { workDir + Config.hyperEdge, outputDir, 
			numReducers, jarFile };
		
		if(!isHyper){
			outputDir = workDir + Config.adjListDir + "." + maxSize;
			opts[0] = workDir + Config.preparedFileDir;
			opts[1] = outputDir;
		}

		ToolRunner.run(conf, new GenAdjListDriver(), opts);
	}
}