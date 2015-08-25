package dbg.hadoop.subgraphs.utils;

import java.util.HashMap;
import java.util.Map;
import java.math.*;

/**
 * This is a encapsulation of the configuration suggestions given in: <br>
 * http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.9.1/bk_installing_manually_book/content/rpm-chap1-11.html <br>
 * By default, we do not consider HBASE
 * @author robeen
 *
 */
class ClusterConfig{
	//Cluser parameters
	private int totalMemPerNodeMB = 32768;
	private int numDisksPerNode = 2;
	private int numCoresPerNode = 8;
	private int numNodes = 4;
	
	//Hadoop and Yarn parameters
	private int numContainers = 0;
	private int ramPerContainer = 0;
	
	private Map<String, Integer> confMap = null;
	
	public ClusterConfig()
	{
		confMap = new HashMap<String, Integer>();
	}
	
	public ClusterConfig(int _totalMemPerNodeMB, int _numDisksPerNode, 
			int _numCoresPerNode, int _numNodes)
	{
		this.totalMemPerNodeMB = _totalMemPerNodeMB;
		this.numDisksPerNode = _numDisksPerNode;
		this.numCoresPerNode = _numCoresPerNode;
		this.numNodes = _numNodes;
		confMap = new HashMap<String, Integer>();
	}
	
	private static final int SIZE_4GB = 0;
	private static final int SIZE_8GB = 1;
	private static final int SIZE_16GB = 2;
	private static final int SIZE_24GB = 3;
	private static final int SIZE_48GB = 4;
	private static final int SIZE_64GB = 5;
	private static final int SIZE_72GB = 6;
	private static final int SIZE_96GB = 7;
	private static final int SIZE_128GB = 8;
	private static final int SIZE_256GB = 9;
	private static final int SIZE_512GB = 10;
	
	/**
	 * A recommended configuration of min container size (in MB)
	 */
	private static final Map<Integer, Integer> minContainerSizeMap = 
		new HashMap<Integer, Integer>(){
        {
            put(SIZE_4GB,  256);  // < 4GB
            put(SIZE_8GB,  512);  // 4GB -> 8GB
            put(SIZE_24GB, 1024); // 8GB -> 24GB
            put(SIZE_48GB, 2048); // >24GB
        }
    };
    
    /**
	 * A recommended configuration of min container size (in MB)
	 */
	private static final Map<Integer, Integer> sysReserveMemMap = 
		new HashMap<Integer, Integer>(){
        {
            put(SIZE_4GB,  1024); 
            put(SIZE_8GB,  2048); 
            put(SIZE_16GB, 2048); 
            put(SIZE_24GB, 4096); 
            put(SIZE_48GB, 6144);
            put(SIZE_64GB, 8192);
            put(SIZE_72GB, 8192);
            put(SIZE_96GB, 12288);
            put(SIZE_128GB, 24576);
            put(SIZE_256GB, 32768);
            put(SIZE_512GB, 65536);
        }
    };
	
	private int getSysReserveMem() {
		if(this.totalMemPerNodeMB <= 4096) {
			return sysReserveMemMap.get(SIZE_4GB);
		}
		else if(this.totalMemPerNodeMB > 4096 && this.totalMemPerNodeMB <= 8192){
			return sysReserveMemMap.get(SIZE_8GB);
		}
		else if(this.totalMemPerNodeMB > 8192 && this.totalMemPerNodeMB <= 16 * 1024){
			return sysReserveMemMap.get(SIZE_16GB);
		}
		else if(this.totalMemPerNodeMB > 16 * 1024 && this.totalMemPerNodeMB <= 24 * 1024){
			return sysReserveMemMap.get(SIZE_24GB);
		}
		else if(this.totalMemPerNodeMB > 24 * 1024 && this.totalMemPerNodeMB <= 48 * 1024){
			return sysReserveMemMap.get(SIZE_48GB);
		}
		else if(this.totalMemPerNodeMB > 48 * 1024 && this.totalMemPerNodeMB <= 64 * 1024){
			return sysReserveMemMap.get(SIZE_64GB);
		}
		else if(this.totalMemPerNodeMB > 64 * 1024 && this.totalMemPerNodeMB <= 72 * 1024){
			return sysReserveMemMap.get(SIZE_72GB);
		}
		else if(this.totalMemPerNodeMB > 72 * 1024 && this.totalMemPerNodeMB <= 96 * 1024){
			return sysReserveMemMap.get(SIZE_96GB);
		}
		else if(this.totalMemPerNodeMB > 96 * 1024 && this.totalMemPerNodeMB <= 128 * 1024){
			return sysReserveMemMap.get(SIZE_128GB);
		}
		else if(this.totalMemPerNodeMB > 128 * 1024 && this.totalMemPerNodeMB <= 256 * 1024){
			return sysReserveMemMap.get(SIZE_96GB);
		}
		else if(this.totalMemPerNodeMB > 256 * 1024 && this.totalMemPerNodeMB <= 512 * 1024){
			return sysReserveMemMap.get(SIZE_512GB);
		}
		else {
			return sysReserveMemMap.get(SIZE_512GB);
		}
		
	}
	
	private int getMinContainerSize()
	{
		int minContainerSize = 0;
		if(this.totalMemPerNodeMB < 4096) {
			minContainerSize = minContainerSizeMap.get(SIZE_4GB);
		}
		else if(this.totalMemPerNodeMB >= 4096 && this.totalMemPerNodeMB < 8192){
			minContainerSize = minContainerSizeMap.get(SIZE_8GB);
		}
		else if(this.totalMemPerNodeMB >= 8192 && this.totalMemPerNodeMB < 24576){
			minContainerSize = minContainerSizeMap.get(SIZE_24GB);
		}
		else if(this.totalMemPerNodeMB >= 24576){
			minContainerSize = minContainerSizeMap.get(SIZE_48GB);
		}
		return minContainerSize;
	}
	
	public void computeNumberContainers()
	{
		int minContainerSize = this.getMinContainerSize();
		int sysReserveMem = this.getSysReserveMem();
		this.numContainers = Math.min(2 * this.numCoresPerNode, 
				(int)Math.round(1.8 * this.numDisksPerNode));
		this.numContainers = Math.min(this.numContainers, 
				(int)Math.round((this.totalMemPerNodeMB - sysReserveMem) / minContainerSize));
	}
	
	public void computeRamPerContainer()
	{
		assert(this.numContainers != 0);
		this.ramPerContainer = Math.max(this.getMinContainerSize(), 
				(int)Math.round((this.totalMemPerNodeMB - this.getSysReserveMem()) / this.numContainers));
	}
	
	public void genConfiguration()
	{
		assert(this.numContainers != 0 && this.ramPerContainer != 0);
		assert(confMap != null);
		confMap.put("yarn.nodemanager.resource.memory-mb", this.numContainers * this.ramPerContainer);
		confMap.put("yarn.scheduler.minimum-allocation-mb", this.ramPerContainer);
		confMap.put("yarn.scheduler.maximum-allocation-mb", this.numContainers * this.ramPerContainer);
		confMap.put("mapreduce.map.memory.mb", this.ramPerContainer);
		confMap.put("mapreduce.reduce.memory.mb", 2 * this.ramPerContainer);
		confMap.put("mapreduce.map.java.opts", (int)(0.8 * this.ramPerContainer));
		confMap.put("mapreduce.reduce.java.opts", (int)(0.8 * 2 * this.ramPerContainer));
	}
	
	public void printConfToXml()
	{
		String res = "";
		assert(confMap != null);
		for(String confItem : confMap.keySet()){
			res += "<property>\n";
			res += "\t<name>" + confItem + "</name>\n";
			res += "\t<value>" + confMap.get(confItem) + "</value>\n";
			res += "</property>\n";
		}
		System.out.print(res);
	}
	
	public static void main(String[] args){
		ClusterConfig conf = new ClusterConfig();
		conf.computeNumberContainers();
		conf.computeRamPerContainer();
		conf.genConfiguration();
		conf.printConfToXml();
	}
}