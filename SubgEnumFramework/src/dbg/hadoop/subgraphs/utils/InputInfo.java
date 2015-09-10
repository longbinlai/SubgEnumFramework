package dbg.hadoop.subgraphs.utils;

import java.io.IOException;

import dbg.hadoop.subgraphs.utils.Utility;

public class InputInfo {
	public String numReducers = "1";
	public String inputFilePath = "";
	public String FileName = "";
	public String separator = "default";
	public String jarFile = "run.jar";
	public String workDir = "";
	public String cliqueNumVertices = "4";
	public String query = "square";
	public long elemSize = 1L;
	public int bfType = Config.EDGE;
	public int maxSize = 0;
	public int cliqueSizeThresh = 0;
	public float falsePositive = (float) 0.001;
	public boolean isUndirected = false;
	public boolean enableBF = true;
	public boolean isHyper = false;
	public boolean isCountOnly = false;
	public boolean isCountPatternOnce = false;
	public boolean isResultCompression = true;
	public boolean isSquareSkip = false;
	public boolean isChordalSquareSkip = false;
	public boolean isFourCliqueSkip = false;
	public boolean isSquarePartition = false;
	public boolean isChordalSquarePartition = false;
	// Use new version clique enumeration functions
	public boolean isEnumCliqueV2 = false;
	public int squarePartitionThresh = 2000;
	public int chordalSquarePartitionThresh = 2000;
	
	public InputInfo(String[] args) throws IOException{
		int valuePos = 0;
		for (int i = 0; i < args.length; ++i) {
			//System.out.println("args[" + i + "] : " + args[i]);
			if(args[i].contains("mapred.input.file")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					inputFilePath = args[i].substring(valuePos);
					System.out.println("mapred.input.file: " + inputFilePath);
				}
				workDir = Utility.getWorkDir(inputFilePath);
				if (workDir.toLowerCase().contains("hdfs")) {
					int pos = workDir.substring("hdfs://".length()).indexOf("/")
							+ "hdfs://".length();
					Utility.setDefaultFS(workDir.substring(0, pos));
				} else {
					Utility.setDefaultFS("");
				}
			}
			else if (args[i].contains("enum.query")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					query = args[i].substring(valuePos);
					System.out.println("enum.query: " + query);
				}
			}
			else if (args[i].contains("mapred.reduce.tasks")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					numReducers = args[i].substring(valuePos);
				}
				assert(Integer.parseInt(numReducers) > 0);
			}
			else if(args[i].contains("graph.undirected")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isUndirected = Boolean.valueOf(args[i].substring(valuePos));
				}
			}
			else if(args[i].contains("mapred.input.key.value.separator")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					separator = args[i].substring(valuePos);
				}
			}
			else if(args[i].contains("jar.file.name")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					jarFile = args[i].substring(valuePos);
				}
			}
			else if (args[i].contains("bloom.filter.false.positive.rate")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					falsePositive = Float.parseFloat(args[i].substring(valuePos));
				}
				assert(falsePositive > 0 && falsePositive <= 1);
			} 
			else if (args[i].contains("bloom.filter.element.size")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					elemSize = Long.parseLong(args[i].substring(valuePos));
					System.out.println("bloom.filter.element.size: " + elemSize);
				}
				assert(elemSize > 1);
			}
			else if (args[i].contains("bloom.filter.type")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					bfType = Integer.parseInt(args[i].substring(valuePos));
					System.out.println("bloom.filter.type: " + bfType);
				}
				assert(bfType == Config.EDGE || bfType == Config.TWINTWIG1);
			}
			else if (args[i].contains("enable.bloom.filter")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					enableBF = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("enable.bloom.filter: " + enableBF);
				}
			}
			else if (args[i].contains("clique.number.vertices")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					cliqueNumVertices = args[i].substring(valuePos);
				}
				assert(Integer.parseInt(cliqueNumVertices) > 3);
			}
			else if(args[i].contains("mapred.clique.size.threshold")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					cliqueSizeThresh = Integer.valueOf(args[i].substring(valuePos));
				}
			}
			else if (args[i].contains("is.hypergraph")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isHyper = Boolean.parseBoolean(args[i].substring(valuePos));
				}
			}
			else if (args[i].contains("count.only")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isCountOnly = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("count.only: " + isCountOnly);
				}
			}
			else if (args[i].contains("count.pattern.once")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isCountPatternOnce = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("count.pattern.once: " + isCountPatternOnce);
				}
			}
			else if(args[i].contains("map.input.max.size")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					maxSize = Integer.valueOf(args[i].substring(valuePos));
					System.out.println("map.input.max.size: " + maxSize);
				}
				assert(maxSize > 0);
			}
			else if (args[i].contains("result.compression")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isResultCompression = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("result.compression: " + isResultCompression);
				}
			}
			// House Enumeration specific settings
			else if (args[i].contains("enum.house.square.partition=")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isSquarePartition = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("enum.house.square.partition: " + isSquarePartition);
				}
			}
			else if (args[i].contains("enum.house.square.partition.thresh=")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					squarePartitionThresh = Integer.parseInt(args[i].substring(valuePos));
					System.out.println("enum.house.square.partition.thresh: " + squarePartitionThresh);
				}
			}
			else if (args[i].contains("skip.square")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isSquareSkip = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("skip.square: " + isSquareSkip);
				}
			}
			// Solar Square Enumeration specific settings
			else if (args[i].contains("enum.solarsquare.chordalsquare.partition=")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isChordalSquarePartition = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("enum.solarsquare.chordalsquare.partition: " + isChordalSquarePartition);
				}
			}
			else if (args[i].contains("enum.solarsquare.chordalsquare.partition.thresh=")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					chordalSquarePartitionThresh = Integer.parseInt(args[i].substring(valuePos));
					System.out.println("enum.solarsquare.chordalsquare.partition.thresh: " + 
							chordalSquarePartitionThresh);
				}
			}
			else if (args[i].contains("skip.chordalsquare")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isChordalSquareSkip = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("skip.chordalsquare: " + isChordalSquareSkip);
				}
			}
			else if (args[i].contains("skip.fourclique")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isFourCliqueSkip = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("skip.fourclique: " + isFourCliqueSkip);
				}
			}
			else if (args[i].contains("enum.clique.v2")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isEnumCliqueV2 = Boolean.parseBoolean(args[i].substring(valuePos));
					System.out.println("enum.clique.v2: " + isEnumCliqueV2);
				}
			}
		}
	}
}