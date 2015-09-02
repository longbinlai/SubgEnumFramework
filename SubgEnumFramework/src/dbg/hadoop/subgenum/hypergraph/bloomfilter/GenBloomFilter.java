package dbg.hadoop.subgenum.hypergraph.bloomfilter;

import java.io.IOException;

import dbg.hadoop.subgraphs.utils.BloomFilterOpr;
import dbg.hadoop.subgraphs.utils.Config;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class GenBloomFilter{
	
	public static void main(String[] args) throws InstantiationException, 
		IllegalAccessException, ClassNotFoundException, IOException{
		run(new InputInfo(args));
	}

	public static void run(InputInfo inputInfo) throws IOException, InstantiationException, 
			IllegalAccessException, ClassNotFoundException{
		
		String inputFilePath = inputInfo.inputFilePath;
		float falsePositive = inputInfo.falsePositive;
		long elemSize = inputInfo.elemSize;
		int bfType = inputInfo.bfType;
		boolean isHyper = inputInfo.isHyper;
		String workDir = inputInfo.workDir;
		
		if(inputFilePath.isEmpty()){
			System.err.println("Input file not specified!");
			System.exit(-1);;
		}
		
		String input = "";
		if (bfType == Config.EDGE) {
			input = isHyper ? workDir + Config.hyperEdge : workDir
					+ Config.preparedFileDir;
		} else {
			input = workDir + Config.distinctTwinTwigDir;
		}
		String output = isHyper ? workDir + Config.hyperGraphBloomFilterDir : workDir + Config.bloomFilterFileDir;
		
		BloomFilterOpr bloomFilterOpr = new BloomFilterOpr(
				input, output,
				falsePositive, elemSize, bfType, workDir);
		
		bloomFilterOpr.createBloomFilter();
		bloomFilterOpr.writeBloomFilter();
	}
}