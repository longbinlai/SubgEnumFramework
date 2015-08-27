package dbg.hadoop.subgraphs.utils;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper.Context;

import dbg.hadoop.subgraphs.utils.HyperVertex;
import dbg.hadoop.subgraphs.io.HyperVertexAdjList;
import dbg.hadoop.subgraphs.io.HVArraySign;
import dbg.hadoop.subgraphs.io.HVArray;
import dbg.hadoop.subgraphs.utils.bloomfilter.utils.BloomFilter;

public class TwinTwigGenerator{
	
	boolean firstAdd;
	boolean enableBF;
	private long cur;
	private long[] largerThanCur;
	private long[] smallerThanCur1;
	private long[] smallerThanCur2;
	private BloomFilter bf;
	
	public TwinTwigGenerator(long _cur, HyperVertexAdjList adjlist){
		this.firstAdd = adjlist.isFirstAdd();
		this.cur = _cur;
		this.largerThanCur = adjlist.getLargeDegreeVertices();
		this.smallerThanCur1 = adjlist.getSmallDegreeVerticesGroup0();
		this.smallerThanCur2 = adjlist.getSmallDegreeVerticesGroup1();
		this.bf = null;
		this.enableBF = false;
	}
	
	public TwinTwigGenerator(long _cur, HyperVertexAdjList adjlist, int degree, boolean isMaxDegree){
		this.firstAdd = adjlist.isFirstAdd();
		this.cur = _cur;
		this.largerThanCur = adjlist.getLargeDegreeVertices(degree, isMaxDegree);
		this.smallerThanCur1 = adjlist.getSmallDegreeVerticesGroup0(_minDegree);
		this.smallerThanCur2 = adjlist.getSmallDegreeVerticesGroup1(degree, isMaxDegree);
		this.bf = null;
		this.enableBF = false;
	}
	
	public TwinTwigGenerator(long _cur, HyperVertexAdjList adjlist, BloomFilter _bf){
		this.firstAdd = adjlist.isFirstAdd();
		this.cur = _cur;
		this.largerThanCur = adjlist.getLargeDegreeVertices();
		this.smallerThanCur1 = adjlist.getSmallDegreeVerticesGroup0();
		this.smallerThanCur2 = adjlist.getSmallDegreeVerticesGroup1();
	
		this.bf = _bf;
		if(this.bf != null){
			this.enableBF = true;
		}
	}
	
	public TwinTwigGenerator(long _cur, HyperVertexAdjList adjlist, BloomFilter _bf, int degree, boolean isMaxDegree){
		this.firstAdd = adjlist.isFirstAdd();
		this.cur = _cur;
		this.largerThanCur = adjlist.getLargeDegreeVertices(degree, isMaxDegree);
		this.smallerThanCur1 = adjlist.getSmallDegreeVerticesGroup0();
		this.smallerThanCur2 = adjlist.getSmallDegreeVerticesGroup1(degree, isMaxDegree);
		this.bf = _bf;
		if(this.bf != null){
			this.enableBF = true;
		}
	}
	
	public void setBloomFilter(BloomFilter _bf){
		this.bf = _bf;
		if(this.bf != null){
			this.enableBF = true;
		}
	}
	
	/**
	 * Generate TwinTwig One with three vertices (A; B, C) rooted on this.cur.
	 * TwinTwig one satisfies that A < B < C;
	 * 
	 * @param context Hadoop Mapper Context
	 * 
	 * @param keyMap We use keyMap to specify which vertices among A, B, C are going <br>
	 * serve the output key. There are seven cases: <br>
	 * 1. keyMap = 000: None of them will be in the key, which of course is a special case;<br>
	 * 2. keyMap = 100 = 4: Only A will be in the key; <br>
	 * 3. keyMap = 010 = 2: Only B will be in the key; <br>
	 * 4. keyMap = 001 = 1: only C will be in the key; <br>
	 * 5. keyMap = 110 = 6: A, B will be in the key; <br>
	 * 6. keyMap = 101 = 5: A, C will be in the key; <br>
	 * 7. keyMap = 011 = 3: A, C will be in the key; <br>
	 * 8. KeyMap = 111 = 7: A, B, C will all be in the key;
	 * 
	 * @param keyMap2 Sometimes it is required to generate two twintwigs at once <br>
	 * using different output key. We will use a different sign(@sign + 1) for the second generator.
	 * 
	 * @param sign The sign associated with the key
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public void genTwinTwigOne(Context context, int sign, byte keyMap, byte keyMap2) 
			throws IOException, InterruptedException{
		if(this.firstAdd){			
			if(allowThree(this.cur)){ // (cur; cur, cur)
				context.write(new HVArraySign(this.cur,
						this.cur, this.cur, sign, keyMap),
						new HVArray(this.cur, this.cur, this.cur, keyMap));
				
				if(keyMap2 != 0){
					context.write(new HVArraySign(
							this.cur, this.cur, this.cur, sign + 1, keyMap2),
							new HVArray(this.cur, this.cur, this.cur, keyMap2));
				}
			}
			int size = this.largerThanCur.length;
			for(int i = 0; i < size; ++i){ // (cur; cur, larger)
				if(allowTwo(this.cur)){
					context.write(new HVArraySign(this.cur, this.cur, 
							this.largerThanCur[i], sign, keyMap), 
							new HVArray(this.cur, 
									this.cur, this.largerThanCur[i], keyMap));
					if(keyMap2 != 0){
						context.write(new HVArraySign(this.cur, this.cur, 
								this.largerThanCur[i], sign + 1, keyMap2), 
								new HVArray(this.cur, 
										this.cur, this.largerThanCur[i], keyMap2));
					}
				}
				// TODO: This should be revised
				if(allowTwo(this.largerThanCur[i], true)){ // cur; larger_i, larger_i
					context.write(new HVArraySign(this.cur, this.largerThanCur[i], 
							this.largerThanCur[i], sign, keyMap), 
							new HVArray(this.cur, 
									this.largerThanCur[i], this.largerThanCur[i], keyMap));
					if(keyMap2 != 0){
						context.write(new HVArraySign(this.cur, this.largerThanCur[i], 
								this.largerThanCur[i], sign + 1, keyMap2), 
								new HVArray(this.cur, 
										this.largerThanCur[i], this.largerThanCur[i], keyMap2));
					}
				}
				for(int j = i + 1; j < size; ++j){
					boolean isOutput = true; // cur; larger_i, larger_j
					if(this.enableBF){ 
						isOutput = this.bf.test(HyperVertex.VertexID(this.largerThanCur[i]),
								HyperVertex.VertexID(this.largerThanCur[j]));
					}
					if(isOutput){
						context.write(new HVArraySign(this.cur, this.largerThanCur[i], 
								this.largerThanCur[j], sign, keyMap), new HVArray(this.cur, 
										this.largerThanCur[i], this.largerThanCur[j], keyMap));
						if(keyMap2 != 0){
							context.write(new HVArraySign(this.cur, this.largerThanCur[i], 
									this.largerThanCur[j], sign + 1, keyMap2), new HVArray(this.cur, 
											this.largerThanCur[i], this.largerThanCur[j], keyMap2));
						}
					}
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void genTwinTwigTwo(Context context, int sign, byte keyMap) throws IOException, InterruptedException{
		if(this.largerThanCur.length == 0){
			return;
		}
		if(allowThree(this.cur)){ // Output {cur, cur, cur}
			context.write(new HVArraySign(this.cur, 
					this.cur, this.cur, sign, keyMap), 
					new HVArray(this.cur, this.cur, this.cur, keyMap));
		}
		if(this.smallerThanCur2.length == 0){ // Output {cur, cur, larger}
			if(allowTwo(this.cur)){
				for(int j = 0; j < this.largerThanCur.length; ++j){ //v3
					context.write(new HVArraySign(this.cur, this.cur, 
							this.largerThanCur[j], sign, keyMap), 
							new HVArray(this.cur, this.cur, 
									this.largerThanCur[j], keyMap));
				}
			}
		}
		for(int i = 0; i < this.smallerThanCur2.length; ++i){ //v2
			if(allowTwo(this.cur)){// Output {cur, smaller, cur}
				context.write(new HVArraySign(this.cur, 
						this.smallerThanCur2[i], this.cur, sign, keyMap), 
						new HVArray(this.cur, this.smallerThanCur2[i], 
								this.cur, keyMap));
			}
			for(int j = 0; j < this.largerThanCur.length; ++j){ //v3
				if(i == 0 && allowTwo(this.cur)){// Output {cur, cur, larger}
					context.write(new HVArraySign(this.cur, this.cur, 
							this.largerThanCur[j], sign, keyMap), 
							new HVArray(this.cur, this.cur, 
									this.largerThanCur[j], keyMap));
				}
				boolean isOutput = true;
				if(this.enableBF){
					isOutput = this.bf.test(HyperVertex.VertexID(this.smallerThanCur2[i]),
							HyperVertex.VertexID(this.largerThanCur[j]));
				}
				if(isOutput){
					context.write(new HVArraySign(this.cur, this.smallerThanCur2[i], 
							this.largerThanCur[j], sign, keyMap), new HVArray(this.cur, 
									this.smallerThanCur2[i], this.largerThanCur[j], keyMap));
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public void genTwinTwigThree(Context context, int sign, byte keyMap) throws IOException, InterruptedException{
		boolean isOutput = true;
		if(this.smallerThanCur1.length == 0) {
			for (int i = 0; i < this.smallerThanCur2.length - 1; ++i) {
				for (int j = i + 1; j < this.smallerThanCur2.length; ++j) {
					if (this.enableBF) {
						isOutput = this.bf.test(
								HyperVertex.VertexID(this.smallerThanCur2[i]),
								HyperVertex.VertexID(this.smallerThanCur2[j]));
					}
					if(isOutput){
						context.write(new HVArraySign(this.cur, this.smallerThanCur2[i], 
								this.smallerThanCur2[j], sign, keyMap), new HVArray(this.cur, 
										this.smallerThanCur2[i], this.smallerThanCur2[j], keyMap));
					}
				}
			}
	    }
	    else{
	    	for (int i = 0; i < this.smallerThanCur1.length; ++i){
	    		for(int j = 0; j < this.smallerThanCur2.length; ++j){
					if (this.enableBF) {
						isOutput = this.bf.test(
								HyperVertex.VertexID(this.smallerThanCur1[i]),
								HyperVertex.VertexID(this.smallerThanCur2[j]));
					}
					if(isOutput){
						context.write(new HVArraySign(this.cur, this.smallerThanCur1[i], 
								this.smallerThanCur2[j], sign, keyMap), new HVArray(this.cur, 
										this.smallerThanCur1[i], this.smallerThanCur2[j], keyMap));
					}
	    		}
	    	}
	    }
	}
	
	/**
	 * Is it allowed to put all the three this.cur in the twintwig
	 * @return
	 */
	public static boolean allowThree(long hypervertex){
		return (HyperVertex.Size(hypervertex) >= 3 && HyperVertex.isClique(hypervertex));
	}
	
	public static boolean allowTwo(long hypervertex){
		return (HyperVertex.Size(hypervertex) >= 2 && HyperVertex.isClique(hypervertex));
	}
	
	public static boolean allowTwo(long hypervertex, boolean isClique){
		if(!isClique){
			return (HyperVertex.Size(hypervertex) >= 2);
		}
		else{
			return allowTwo(hypervertex);
		}
	}
	
	public void clear(){
		this.largerThanCur = null;
		this.smallerThanCur1 = null;
		this.smallerThanCur2 = null;
	}
}