package dbg.hadoop.subgenum.maximalclique;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class MaximalClique{
	private static InputInfo inputInfo = null;
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = inputInfo.enableBF;
		int cliqueSizeThresh = inputInfo.cliqueSizeThresh;
		
		String jarFile = inputInfo.jarFile;
		String numReducers = inputInfo.numReducers;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		
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
		
		Utility.getFS().delete(new Path(s1Output));
		Utility.getFS().delete(new Path(s2Output));
		Utility.getFS().delete(new Path(s3Output));
	}
}