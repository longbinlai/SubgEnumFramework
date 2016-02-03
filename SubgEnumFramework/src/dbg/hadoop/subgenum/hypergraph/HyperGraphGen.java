package dbg.hadoop.subgenum.hypergraph;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.Utility;

public class HyperGraphGen{

	public static void main(String[] args) throws Exception{
		int valuePos = 0;
		String numReducers = "1";
		String inputFilePath = "";
		String jarFile = "";
		int thresh = 0;
		//String outputCompress = "false";
		
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
					inputFilePath = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("mapred.hypergraph.threshold")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					thresh = Integer.valueOf(args[i].substring(valuePos));
				}
			}
			else if(args[i].contains("jar.file.name")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					jarFile = args[i].substring(valuePos);
				}
			}

		}
		
		String dir = Utility.getWorkDir(inputFilePath);
		
		if (dir.toLowerCase().contains("hdfs")) {
			int pos = dir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(dir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		//String stageZeroOutputDir = dir + "hyper.stage0.out";
		String stageOneOutputDir = dir + "hyper.stage1.out";
		String stageTwoOutputDir = dir + "hyper.stage2.out";
		String stageThreeOutputDir = dir + Config.hyperVertex;
		String stageFourOutputDir = dir + "hyper.stage4.out";
		String stageFiveOutputDir = dir + "hyper.stage5.out";
		String stageSixOutputDir = dir + Config.hyperEdge;
		
		Configuration conf = new Configuration();
		if(thresh != 0){
			conf.setInt("mapred.hypergraph.threshold", thresh);
		}
		// Stage 1
		String[] opts = { dir + Config.adjListDir + ".0", stageOneOutputDir, numReducers, jarFile};
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		ToolRunner.run(conf, new HyperGraphStageOneDriver(), opts);
		
		// Stage 2
		// The parameters: <inputDir> <outputDir> <numReducers> <jarFile>
		opts[1] = stageTwoOutputDir;
		ToolRunner.run(conf, new HyperGraphStageTwoDriver(), opts);
		
		// Stage 3
		// args: <stage1_output> <stage2_output> <output> <numReducers> <jarFile> 
		String[] stageThreeOpts = {stageOneOutputDir, stageTwoOutputDir, stageThreeOutputDir, numReducers, jarFile};
		ToolRunner.run(conf, new HyperGraphStageThreeDriver(), stageThreeOpts);
		
		// Stage 4
		String[] stageFourOpts = {dir + Config.adjListDir + ".0", stageThreeOutputDir, stageFourOutputDir, numReducers, jarFile};
		ToolRunner.run(conf, new HyperGraphStageFourDriver(), stageFourOpts);
		
		// Stage 5
		String[] stageFiveOpts = {stageFourOutputDir, stageThreeOutputDir, stageFiveOutputDir, numReducers, jarFile};
		ToolRunner.run(conf, new HyperGraphStageFiveDriver(), stageFiveOpts);
		
		// Stage 6: Removing duplicated results from stage 5
		String[] stageSixOpts = {stageFiveOutputDir, stageSixOutputDir, numReducers, jarFile};
		ToolRunner.run(conf, new HyperGraphStageSixDriver(), stageSixOpts);
				
		// Delete intemediate results
		Utility.getFS().delete(new Path(stageOneOutputDir), true);
		Utility.getFS().delete(new Path(stageTwoOutputDir), true);
		//Utility.getFS().delete(new Path(stageThreeOutputDir), true);
		Utility.getFS().delete(new Path(stageFourOutputDir), true);
		Utility.getFS().delete(new Path(stageFiveOutputDir), true);
		//Utility.getFS().delete(new Path(stageSixOutputDir), true);
	}
}

