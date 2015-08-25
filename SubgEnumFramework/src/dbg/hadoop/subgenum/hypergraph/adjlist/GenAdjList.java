package dbg.hadoop.subgenum.hypergraph.adjlist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;

public class GenAdjList{
	public static void main(String[] args) throws Exception{
		//StopWatch timer = new StopWatch();
		//timer.start();
		int valuePos = 0;
		String numReducers = "1";
		String inputFile = "";
		String jarFile = "";
		int maxSize = 0;
		boolean isHyper = true;
		
		//String outputFile = "";
		for (int i = 0; i < args.length; ++i) {
			// System.out.println("args[" + i + "] = " + args[i]);
			if (args[i].contains("mapred.reduce.tasks")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					numReducers = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("mapred.input.file")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					inputFile = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("jar.file.name")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					jarFile = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("map.input.max.size")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					maxSize = Integer.valueOf(args[i].substring(valuePos));
				}
			}
			else if(args[i].contains("is.hypergraph")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isHyper = Boolean.valueOf(args[i].substring(valuePos));
				}
			}
		}
		
		if(inputFile.isEmpty()){
			System.err.println("Input file not specified!");
			return;
		}
		
		String workDir = Utility.getWorkDir(inputFile);
		
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