package dbg.hadoop.subgenum.frame;


import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import dbg.hadoop.subgraphs.utils.InputInfo;
import dbg.hadoop.subgraphs.utils.Utility;

public class MainEntry{
	private static InputInfo inputInfo = null;
	private static Logger log = Logger.getLogger(MainEntry.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String workDir = inputInfo.workDir;
		String query = inputInfo.query.toLowerCase();
		long startTime = 0;
		long endTime = 0;
		// Square is query: q1
		if(query.compareTo("square") == 0 || query.compareTo("q1") == 0){
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.res"))){
				Utility.getFS().delete(new Path(workDir + "frame.square.res"));
			}
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.cnt"))){
				Utility.getFS().delete(new Path(workDir + "frame.square.cnt"));
			}
			log.info("Start enumerating square...");
			startTime = System.currentTimeMillis();
			
			EnumSquare.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			EnumSquare.countOnce(inputInfo);
		}
		// Chordal Square is query: q2
		else if(query.compareTo("chordalsquare") == 0 || query.compareTo("q2") == 0){
			// Delete existed output
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.res"))) {
				Utility.getFS().delete(new Path(workDir + "frame.csquare.res"));
			}
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.cnt"))) {
				Utility.getFS().delete(new Path(workDir + "frame.csquare.cnt"));
			}
			log.info("Start enumerating chordal square...");
			startTime = System.currentTimeMillis();
			
			EnumChordalSquare.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-chordalsquare] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			
			EnumChordalSquare.countOnce(inputInfo);
		}
		// k-clique is query: q3
		else if (query.compareTo("clique") == 0 || query.compareTo("q3") == 0) {
			// Delete existed output
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.clique.res"))){
				Utility.getFS().delete(new Path(workDir + "frame.clique.res"));
			}
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.clique.cnt"))){
				Utility.getFS().delete(new Path(workDir + "frame.clique.cnt"));
			}
			log.info("Start enumerating " + inputInfo.cliqueNumVertices + "-clique...");
			startTime = System.currentTimeMillis();
			
			if(inputInfo.isResultCompression) {
				log.info("Enumerating clique V2 ...");
				EnumCliqueV2.run(inputInfo);
			}
			else {
				EnumClique.run(inputInfo);
			}
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-" + inputInfo.cliqueNumVertices + "clique] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			if(inputInfo.isResultCompression)
				EnumCliqueV2.countOnce(inputInfo); // V2 is the compress version
			else
				EnumClique.countOnce(inputInfo);
		}
		// House is query: q4
		else if (query.compareTo("house") == 0 || query.compareTo("q4") == 0) {
			if(!inputInfo.isSquareSkip){
				if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.res"))){
					Utility.getFS().delete(new Path(workDir + "frame.square.res"));
				}
			}
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.square.res.part"))){
				Utility.getFS().delete(new Path(workDir + "frame.square.res.part"));
			}
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.house.res"))){
				Utility.getFS().delete(new Path(workDir + "frame.house.res"));
			}
			if(Utility.getFS().isDirectory(new Path(workDir + "frame.house.cnt"))){
				Utility.getFS().delete(new Path(workDir + "frame.house.cnt"));
			}
			log.info("Start enumerating house...");
			startTime = System.currentTimeMillis();
			
			EnumHouse.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-house] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			
			EnumHouse.countOnce(inputInfo);
		}
		// Solar Square is query: q5
		else if (query.compareTo("solarsquare") == 0 || query.compareTo("q5") == 0) {
			// Delete existed output
			if(!inputInfo.isChordalSquareSkip){
				if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.res"))) {
					Utility.getFS().delete(new Path(workDir + "frame.csquare.res"));
				}
			}
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.csquare.res.part"))) {
				Utility.getFS().delete(new Path(workDir + "frame.csquare.res.part"));
			}
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.solarsquare.res"))) {
				Utility.getFS().delete(new Path(workDir + "frame.solarsquare.res"));
			}
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.solarsquare.cnt"))) {
				Utility.getFS().delete(new Path(workDir + "frame.solarsquare.cnt"));
			}
			if (inputInfo.isLeftDeep) {
				if (Utility.getFS().isDirectory(
						new Path(workDir + "frame.solarsquare.res.1"))) {
					Utility.getFS().delete(
							new Path(workDir + "frame.solarsquare.res.1"));
				}
				if (Utility.getFS().isDirectory(
						new Path(workDir + "frame.solarsquare.res.2"))) {
					Utility.getFS().delete(
							new Path(workDir + "frame.solarsquare.res.2"));
				}
			}
			log.info("Start enumerating solar square...");
			startTime = System.currentTimeMillis();
			
			if(inputInfo.isLeftDeep)
				EnumSolarSquareLD.run(inputInfo);
			else
				EnumSolarSquare.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-solarsquare] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			
			EnumSolarSquare.countOnce(inputInfo);
		}
		// Twin Triangle is q6
		else if (query.compareTo("twintriangle") == 0 || query.compareTo("q6") == 0) {
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.twintriangle.res"))) {
				Utility.getFS().delete(new Path(workDir + "frame.twintriangle.res"));
			}
			if (Utility.getFS().isDirectory(new Path(workDir + "frame.twintriangle.cnt"))) {
				Utility.getFS().delete(new Path(workDir + "frame.twintriangle.cnt"));
			}
			log.info("Start enumerating twin triangle...");
			startTime = System.currentTimeMillis();
			
			EnumTwinTriangle.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-twintriangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			EnumTwinTriangle.countOnce(inputInfo);
		}
		else if (query.compareTo("near5clique") == 0 || query.compareTo("q7") == 0) {
			if(!inputInfo.isFourCliqueSkip){
				if (Utility.getFS().isDirectory(new Path(workDir + "frame.clique.res"))) {
					Utility.getFS().delete(new Path(workDir + "frame.clique.res"));
				}
			}
			Utility.getFS().delete(new Path(workDir + "frame.near5clique.res"));
			Utility.getFS().delete(new Path(workDir + "frame.near5clique.cnt"));
			log.info("Start enumerating near5clique...");
			startTime = System.currentTimeMillis();
			
			EnumNear5Clique.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-near5clique] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			EnumNear5Clique.countOnce(inputInfo);
		}
		else if (query.compareTo("tcsquare") == 0 || query.compareTo("q8") == 0) {
			Utility.getFS().delete(new Path(workDir + "frame.tcsquare.res.1"));
			Utility.getFS().delete(new Path(workDir + "frame.tcsquare.res.2"));
			Utility.getFS().delete(new Path(workDir + "frame.tcsquare.res"));
			Utility.getFS().delete(new Path(workDir + "frame.tcsquare.cnt"));
			log.info("Start enumerating twin chordal square...");
			startTime = System.currentTimeMillis();
			
			System.out.println(inputInfo.isLeftDeep);
			
			if(inputInfo.isLeftDeep)
				EnumTwinCSquareLD.run(inputInfo);
			else 
				EnumTwinCSquare.run(inputInfo);
			
			endTime=System.currentTimeMillis();
			log.info("[Frame-tcsquare] Time elapsed: " + (endTime - startTime) / 1000 + "s");
			EnumTwinCSquare.countOnce(inputInfo);
		}
		else {
			System.err.println("Please specify enum.query=[...];");
			System.err.println("Supported queries are: square, " +
					"chordalsquare, clique, house, solarsquare, " +
					"twintriangle, near5clique, tcsquare;");
			System.exit(0);
		}
	}
}