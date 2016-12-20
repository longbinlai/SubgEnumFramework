package dbg.hadoop.subgenum.prepare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import dbg.hadoop.subgenum.frame.MainEntry;
import dbg.hadoop.subgenum.hypergraph.adjlist.GenAdjList;
import dbg.hadoop.subgenum.hypergraph.bloomfilter.DistinctTwinTwig;
import dbg.hadoop.subgenum.hypergraph.bloomfilter.GenBloomFilter;
import dbg.hadoop.subgenum.hypergraph.triangle.Triangle;
import dbg.hadoop.subgenum.maximalclique.MaximalClique;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class PrepareData{
	private static InputInfo inputInfo = null;
	private static Logger log = Logger.getLogger(PrepareData.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		int maxSize = inputInfo.maxSize;
		long begin = 0, end = 0;
		
		
		String numReducers = inputInfo.numReducers;
		String inputFilePath = inputInfo.inputFilePath;
		String jarFile = inputInfo.jarFile;
		// default as tab, alternative choices are: space, comma, semicolon
		String separator = inputInfo.separator;
		boolean isUndirected = inputInfo.isUndirected;
		String dir = inputInfo.workDir;
		

		if(Utility.getFS().isDirectory(new Path(dir + Config.undirectGraphDir))){
			Utility.getFS().delete(new Path(dir + Config.undirectGraphDir));
		}

		if(Utility.getFS().isDirectory(new Path(dir + Config.degreeFileDir))){
			Utility.getFS().delete(new Path(dir + Config.degreeFileDir));
		}
		if(Utility.getFS().isDirectory(new Path(dir + Config.preparedFileDir + ".tmp"))){
			Utility.getFS().delete(new Path(dir + Config.preparedFileDir + ".tmp"));
		}
		if(Utility.getFS().isDirectory(new Path(dir + Config.preparedFileDir))){
			Utility.getFS().delete(new Path(dir + Config.preparedFileDir));
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
		
		if(Utility.getFS().isDirectory(new Path(dir + Config.undirectGraphDir))){
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

		
		// Generate adjlist
		inputInfo.maxSize = 0;
		GenAdjList.run(inputInfo);
		inputInfo.maxSize = maxSize;
		GenAdjList.run(inputInfo);
		
		// Generate bloom filter
		DistinctTwinTwig.run(inputInfo);
		inputInfo.bfType = Config.EDGE;
		GenBloomFilter.run(inputInfo);
		inputInfo.bfType = Config.TWINTWIG1;
		GenBloomFilter.run(inputInfo);
		

		log.info("Start enumerating maximal cliques..");
		begin = System.currentTimeMillis();
		// Generate Maximal Clique
		MaximalClique.run(inputInfo);
		end = System.currentTimeMillis();
		
		log.info("[Pre-Clique] Time elapsed: " + (end - begin) / 1000 + "s");
		
		log.info("Star enumrating triangles to construct local graphs.");
		begin = System.currentTimeMillis();
		// Generate Triangle
		Triangle.run(inputInfo);
		end = System.currentTimeMillis();
		
		log.info("[Pre-Triangle] Time elapsed: " + (end - begin) / 1000 + "s");
	}
}