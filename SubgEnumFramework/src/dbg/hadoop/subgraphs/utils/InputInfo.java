package dbg.hadoop.subgraphs.utils;

import dbg.hadoop.subgraphs.utils.Utility;

public class InputInfo {
	public String numReducers = "1";
	public String inputFilePath = "";
	public String FileName = "";
	public String separator = "default";
	public String jarFile = "run.jar";
	public String workDir = "";
	public String cliqueNumVertices = "4";
	public long elemSize = 1L;
	public int bfType = Config.EDGE;
	public int maxSize = 0;
	public int cliqueSizeThresh = 0;
	public float falsePositive = (float) 0.001;
	public boolean isUndirected = false;
	public boolean enableBF = false;
	public boolean isHyper = false;
	public boolean isCountOnly = true;
	public boolean isCountCliqueOnce = false;
	
	public InputInfo(String[] args){
		int valuePos = 0;
		for (int i = 0; i < args.length; ++i) {
			System.out.println("args[" + i + "] = " + args[i]);
			if (args[i].contains("mapred.reduce.tasks")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					numReducers = args[i].substring(valuePos);
				}
				assert(Integer.parseInt(numReducers) > 0);
			}
			else if(args[i].contains("mapred.input.file")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					inputFilePath = args[i].substring(valuePos);
				}
				workDir = Utility.getWorkDir(inputFilePath);
				FileName = Utility.getFileName(inputFilePath);
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
				}
				assert(elemSize > 1);
			}
			else if (args[i].contains("bloom.filter.type")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					bfType = Integer.parseInt(args[i].substring(valuePos));
				}
				assert(bfType == Config.EDGE || bfType == Config.TWINTWIG1);
			}
			else if (args[i].contains("enable.bloom.filter")) {
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					enableBF = Boolean.parseBoolean(args[i].substring(valuePos));
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
				}
			}
			else if (args[i].contains("count.clique.once")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					isCountCliqueOnce = Boolean.parseBoolean(args[i].substring(valuePos));
				}
			}
			else if(args[i].contains("map.input.max.size")){
				valuePos = args[i].lastIndexOf("=") + 1;
				if (valuePos != 0) {
					maxSize = Integer.valueOf(args[i].substring(valuePos));
				}
				assert(maxSize > 0);
			}
		}
	}
	public InputInfo(){}

}