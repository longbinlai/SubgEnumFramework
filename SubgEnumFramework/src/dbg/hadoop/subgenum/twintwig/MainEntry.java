package dbg.hadoop.subgenum.twintwig;

import org.apache.log4j.Logger;
import dbg.hadoop.subgraphs.utils.InputInfo;

public class MainEntry{
	private static InputInfo inputInfo = null;
	private static Logger log = Logger.getLogger(MainEntry.class);
	
	public static void main(String[] args) throws Exception{
		inputInfo = new InputInfo(args);
		String query = inputInfo.query.toLowerCase();
		long startTime = 0;
		long endTime = 0;
		// Square is query: q1
		if(query.compareTo("square") == 0 || query.compareTo("q1") == 0){
			log.info("TwinTwig: Start enumerating square...");
			startTime = System.currentTimeMillis();	
			Square.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-square] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		// Chordal Square is query: q2
		else if(query.compareTo("chordalsquare") == 0 || query.compareTo("q2") == 0){
			log.info("TwinTwig: Start enumerating chordal square...");
			startTime = System.currentTimeMillis();
			ChordalSquare.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-chordalsquare] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		// k-clique is query: q3
		else if (query.compareTo("4clique") == 0) {
			log.info("TwinTwig: Start enumerating 4-clique...");
			startTime = System.currentTimeMillis();
			FourClique.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-4Clique] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		else if (query.compareTo("5clique") == 0) {
			log.info("TwinTwig: Start enumerating 5-clique...");
			startTime = System.currentTimeMillis();
			FiveClique.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-5Clique] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		// House is query: q4
		else if (query.compareTo("house") == 0 || query.compareTo("q4") == 0) {
			log.info("TwinTwig: Start enumerating house...");
			startTime = System.currentTimeMillis();
			House.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-house] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		// Solar Square is query: q5
		else if (query.compareTo("solarsquare") == 0 || query.compareTo("q5") == 0) {
			log.info("TwinTwig: Start enumerating solar square...");
			startTime = System.currentTimeMillis();
			SolarSquare.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-solarsquare] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
		// Twin Triangle is q6
		else if (query.compareTo("twintriangle") == 0 || query.compareTo("q6") == 0) {
			log.info("TwinTwig: Start enumerating twin triangle...");
			startTime = System.currentTimeMillis();
			TwinTriangle.run(inputInfo);
			endTime=System.currentTimeMillis();
			log.info("[TwinTwig-twintriangle] Time elapsed: " + (endTime - startTime) / 1000 + "s");
		}
	}
}