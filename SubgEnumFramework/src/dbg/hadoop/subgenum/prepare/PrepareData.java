package dbg.hadoop.subgenum.prepare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import dbg.hadoop.subgenum.hypergraph.adjlist.GenAdjListDriver;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class PrepareData{
	private static InputInfo inputInfo = null;
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);

		String numReducers = inputInfo.numReducers;
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		// default as tab, alternative choices are: space, comma, semicolon
		String separator = inputInfo.separator;
		boolean isUndirected = inputInfo.isUndirected;
		int maxSize = inputInfo.maxSize;
		String dir = inputInfo.workDir;
		
		if (dir.toLowerCase().contains("hdfs")) {
			int pos = dir.substring("hdfs://".length()).indexOf("/")
					+ "hdfs://".length();
			Utility.setDefaultFS(dir.substring(0, pos));
		} else {
			Utility.setDefaultFS("");
		}
		
		// Initially, turn the graph into its undirected version if necessary
		// and change the delimiter from anything to tab
		// The parameters: <inputfile> <outputDir> <numReducers> <seperator> <jarFile>
		if(!isUndirected){
			String[] undirectedOpts = {inputFilePath, dir + Config.undirectGraphDir, 
					numReducers, separator, jarFile};
			ToolRunner.run(new Configuration(), new UndirectGraphDriver(), undirectedOpts);
			// change the graph input path to the undirected output path
			inputFilePath = dir + Config.undirectGraphDir;
		}
		
		
		// First, generate the degree
		// The parameters: <inputfile> <outputDir> <numReducers> <jarFile>
		String[] genDegreeOpts = { inputFilePath, dir + Config.degreeFileDir, 
				numReducers, jarFile};
		ToolRunner.run(new Configuration(), new GenDegreeDriver(), genDegreeOpts);
		
		// Second, replace the end point of each edge with a hypervertex which
		// encapsulate vertexid + degree
		// Stage One: Replace the left end point
		// The parameters: <degreefile> <graphfile> <outputDir> <numReducers> <jarFile>
		String s1OutputDir = dir + Config.preparedFileDir + ".tmp";
		String[] prepareDataOpts = { dir + Config.degreeFileDir, inputFilePath,  s1OutputDir, 
				numReducers, jarFile};
		ToolRunner.run(new Configuration(), new PrepareDataS1Driver(), prepareDataOpts);
		
		// Stage Two: Replace the right end point and end up with correct order
		// The parameters: <degreefile> <stageOneOutputDir> <outputDir> <numReducers> <jarFile>
		prepareDataOpts[1] = s1OutputDir;
		prepareDataOpts[2] = dir + Config.preparedFileDir;
		ToolRunner.run(new Configuration(), new PrepareDataS2Driver(), prepareDataOpts);
		
		Utility.getFS().delete(new Path(s1OutputDir));
		
		Configuration conf = new Configuration();
		conf.setInt("map.input.max.size", maxSize);
		// Third, Generating adjlist
		String[] adjListOpts = { dir + Config.preparedFileDir, 
				dir + Config.adjListDir + "." + maxSize, numReducers, jarFile };
		ToolRunner.run(conf, new GenAdjListDriver(), adjListOpts);
	}
}