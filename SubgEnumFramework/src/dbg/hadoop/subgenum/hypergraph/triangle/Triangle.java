package dbg.hadoop.subgenum.hypergraph.triangle;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class Triangle{
	
	public static void main(String[] args) throws Exception{
		run(new InputInfo(args));
	}
	
	public static void run(InputInfo inputInfo) throws Exception{
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		boolean enableBF = true;
		boolean isHyper = inputInfo.isHyper;
		int maxSize = inputInfo.maxSize;
		
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
		
		String output = workDir + "triangle.res";
		
		if(Utility.getFS().isDirectory(new Path(output))){
			Utility.getFS().delete(new Path(output));
		}
		
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