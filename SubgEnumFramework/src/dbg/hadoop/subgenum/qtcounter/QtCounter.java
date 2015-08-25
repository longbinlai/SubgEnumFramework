package dbg.hadoop.subgenum.qtcounter;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;

public class QtCounter{
	public static void main(String[] args) throws Exception{
		String inputFilePath = "";
		double falsePositive = 0.001;
		boolean enableBF = true;
		int cliqueSize = 4;
		
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
			if(args[i].contains("mapred.clique.size")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					cliqueSize = Integer.valueOf(args[i].substring(valuePos));
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
		
		String s1Output = workDir + "qt.1.out";
		String s2Output = workDir + "qt.2.out";
		String s3Output = workDir + "qt." + cliqueSize + "Clique.counter.res";
		
		Configuration conf = new Configuration();
		conf.setBoolean("enable.bloom.filter", enableBF);
		conf.setFloat("bloom.filter.false.positive.rate", (float)falsePositive);
		if(enableBF){
			String bloomFilterFileName = "bloomFilter." + Config.EDGE + "." + falsePositive;
			DistributedCache.addCacheFile(new URI(new Path(workDir).toUri()
				.toString() + "/" + Config.hyperGraphBloomFilterDir + "/" + bloomFilterFileName), conf);
		}
		
		conf.setInt("mapred.clique.size", cliqueSize);
		
		// The parameters: <graphFileDir> <adjListDir> <outputDir> <numReducers> <jarFile>
		String opt1[] = { workDir + Config.preparedFileDir, workDir + Config.adjListDir + ".0", 
				s1Output, numReducers, jarFile};
		
		ToolRunner.run(conf, new QtS1Driver(), opt1);
		
		String opt2[] = { s1Output, s2Output, numReducers, jarFile};
		ToolRunner.run(conf, new QtS2Driver(), opt2);
		
		//String opt3[] = { s2Output, s3Output, numReducers, jarFile};
		//ToolRunner.run(conf, new QtS3Driver(), opt3);
		
		Utility.getFS().delete(new Path(s1Output));
		//Utility.getFS().delete(new Path(s2Output));
	}
}