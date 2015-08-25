package dbg.hadoop.subgenum.maximalclique;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;

public class MaximalClique{
	public static void main(String[] args) throws Exception{
		String inputFilePath = "";
		double falsePositive = 0.001;
		boolean enableBF = true;
		int cliqueSizeThresh = 20;
		
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
			else if(args[i].contains("mapred.input.file")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					inputFilePath = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("mapred.clique.size.threshold")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					cliqueSizeThresh = Integer.valueOf(args[i].substring(valuePos));
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
			else if(args[i].contains("jar.file.name")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					jarFile = args[i].substring(valuePos);
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
		
		String s1Output = workDir + "clique.1.out";
		String s2Output = workDir + "clique.2.out";
		String s3Output = workDir + "clique.3.out";
		String s4Output = workDir + Config.cliques;
		//String s5Output = workDir + Config.hyperEdge;
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		
		conf.setInt("mapred.clique.size.threshold", cliqueSizeThresh);
		
		if (enableBF) {
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri().toString() + "/"
					+ Config.bloomFilterFileDir + "/" + bloomFilterFileName), conf);
		}
		
		// The parameters: <graphFileDir> <adjListDir> <outputDir> <numReducers> <jarFile>
		String opt1[] = { workDir + Config.preparedFileDir, workDir + Config.adjListDir + ".0", 
				s1Output, numReducers, jarFile};
		
		ToolRunner.run(conf, new MCliqueS1Driver(), opt1);
		
		String opt2[] = { s1Output, s2Output, numReducers, jarFile};
		ToolRunner.run(conf, new MCliqueS2Driver(), opt2);
		
		String opt3[] = { workDir + Config.degreeFileDir, s2Output, s3Output, numReducers, jarFile};
		ToolRunner.run(conf, new MCliqueS3Driver(), opt3);
		
		String opt4[] = { s3Output, s4Output, numReducers, jarFile};
		ToolRunner.run(conf, new MCliqueS4Driver(), opt4);
	}
}