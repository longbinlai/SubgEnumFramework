package dbg.hadoop.subgenum.hypergraph.triangle;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;

public class Triangle{
	public static void main(String[] args) throws Exception{
		String inputFilePath = "";
		double falsePositive = 0.001;
		boolean enableBF = true;
		boolean isHyper = false;
		int maxSize = 0;
		
		String jarFile = "";
		String numReducers = "";
		
		int valuePos = 0;
		for (int i = 0; i < args.length; ++i) {
			if (args[i].contains("mapred.reduce.tasks")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					numReducers = args[i].substring(valuePos);
				}
			}
			if(args[i].contains("mapred.input.file")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					inputFilePath = args[i].substring(valuePos);
				}
			}
			else if (args[i].contains("bloom.filter.false.positive.rate")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					falsePositive = Double.parseDouble(args[i].substring(valuePos));
				}
			} 
			else if (args[i].contains("enable.bloom.filter")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					enableBF = Boolean.parseBoolean(args[i].substring(valuePos));
				}
			}
			else if (args[i].contains("is.hypergraph")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isHyper = Boolean.valueOf(args[i].substring(valuePos));
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
		}
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String workDir = Utility.getWorkDir(inputFilePath);
		
		if (workDir.toLowerCase().contains("hdfs")) {
			int pos = workDir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(workDir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		String output = workDir + "triangle.res";
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		
		if(enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
					.toString() + "/" + Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		String adjListDir = isHyper ? workDir + Config.hyperGraphAdjList + "." + maxSize :
				workDir + Config.adjListDir + "." + maxSize;
		
		// The parameters: <graphFileDir> <adjListDir> <outputDir> <numReducers> <jarFile>
		String opts[] = { workDir + Config.preparedFileDir, adjListDir, 
				output, numReducers, jarFile};
		
		ToolRunner.run(conf, new TriangleDriver(), opts);
	}
}