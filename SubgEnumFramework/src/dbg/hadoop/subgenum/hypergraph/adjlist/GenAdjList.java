package dbg.hadoop.subgenum.hypergraph.adjlist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class GenAdjList{
	
	public static void main(String[] args) throws Exception {
		run(new InputInfo(args));
	}

	public static void run(InputInfo inputInfo) throws Exception{
		String numReducers = inputInfo.numReducers;
		String inputFile = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		int maxSize = inputInfo.maxSize;
		boolean isHyper = inputInfo.isHyper;
		String workDir = inputInfo.workDir;
		
		if(inputFile.isEmpty()){
			System.err.println("Input file not specified!");
			return;
		}
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		Configuration conf = new Configuration();
		conf.setInt("map.input.max.size", maxSize);
		
		String outputDir = workDir + Config.hyperGraphAdjList + "." + maxSize;
		String[] opts = { workDir + Config.hyperEdge, outputDir, numReducers, jarFile };

		if (!isHyper) {
			outputDir = workDir + Config.adjListDir + "." + maxSize;
			opts[0] = workDir + Config.preparedFileDir;
			opts[1] = outputDir;
		}
		
		if(Utility.getFS().isDirectory(new Path(outputDir)))
			Utility.getFS().delete(new Path(outputDir));
		if(Utility.getFS().isDirectory(new Path(outputDir + ".tmp")))
			Utility.getFS().delete(new Path(outputDir + ".tmp"));
		
		if (maxSize == 0) {
			ToolRunner.run(conf, new GenAdjListDriver(), opts);
		}
		else{
			opts[1] = outputDir + ".tmp";
			ToolRunner.run(conf, new GenAdjListDriver(), opts);
			opts[0] = outputDir + ".tmp";
			opts[1] = outputDir;
			ToolRunner.run(conf, new GenAdjListRandomShuffleDriver(), opts);
			
			Utility.getFS().delete(new Path(outputDir + ".tmp"));
		}
	}
}