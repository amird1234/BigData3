package univ.bigdata.course;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import comparators.PageRankComperator;
import enums.CommandType;
import scala.Tuple2;
import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.providers.MovieRecommands;

public class MainRunner {

    public static void main(String[] args) {
    	MoviesStorage moviesStorage = null;
    	//    	CommandType query = CommandType.fromString(args[0]);
    	CommandType query = CommandType.fromString("recommend");
    	
    	switch (query) {
		case COMMANDS:
			String commandsFileName = args[1];
			try(BufferedReader br = new BufferedReader(new FileReader(commandsFileName))) {
			    String inputPath = br.readLine();
			    String outputPath = br.readLine();
			    
		    	//initialize movie storage and spark context
		    	moviesStorage = new MoviesStorage(inputPath);
		    	
		    	moviesStorage.startQueryRunner(outputPath);
		    	
		    	//read first query
		    	String line = br.readLine();
		    	//while we have a new query
		        while (line != null) {
		        	//perform the query
		        	moviesStorage.runQuery(line);
		        	//read next query
		            line = br.readLine();
		        } 
		        br.close();
			}catch (Exception e) {
				throw new IllegalArgumentException("Program Second argument is illegal.");
			}finally {
				if(moviesStorage != null){
					moviesStorage.closeJavaSparkContext();
				}
			} 
			
			break;
		case RECOMMEND:
			//String RecommendationFileName = args[1];
			String RecommendationFileName = "recommend.txt";
			
			
			try(BufferedReader br = new BufferedReader(new FileReader(RecommendationFileName))) {
			    String inputPath = br.readLine();
			    String outputPath = br.readLine();
			    
			    PrintStream printer  = new PrintStream (new File(outputPath));
			    
		    	//initialize movie storage and spark context
		    	MovieRecommands mr = new MovieRecommands(inputPath);
		    	mr.recommend(RecommendationFileName).forEach(a->printer.println(a.printRecommendations()));
//		    	moviesStorage.startQueryRunner(outputPath);
//		    	
//		    	//read first user id
//		    	String line = br.readLine();
//		    	//while we have a new user id
//		        while (line != null) {
//		        	//get recommendations
//		        	//TODO: Need to implement the recommendations
//		        	//read next user id
//		            line = br.readLine();
//		        } 
//		        br.close();
			}catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Program Second argument is illegal.");
			}finally {
				if(moviesStorage != null){
					moviesStorage.closeJavaSparkContext();
				}
			} 
			break;
			
		case MAP_COMMAND:
			
			break;
			
		case PAGE_RANK:

			String movieSimpleFile = args[1];
	    	//initialize movie storage and spark context
	    	moviesStorage = new MoviesStorage(movieSimpleFile);
	    	
			JavaRDD<String> Edges = null;
			//TODO: Need to insert Carmi Code to generate JavaRDD<String>
			try {
				List<Tuple2<String, Double>> PRresults = JavaPageRank.Rank(Edges, 100);
				
				PRresults.sort(new PageRankComperator());
				
				if(PRresults.size() > 100){
					PRresults = PRresults.subList(0, 99);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
	    	
	    	moviesStorage.closeJavaSparkContext();
			break;
		default:
			throw new IllegalArgumentException("Ptogram First argument is illegal, needs to be commands/recommend/mappagerank");
		
		}
    }
}
