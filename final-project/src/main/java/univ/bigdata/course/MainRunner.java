package univ.bigdata.course;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import comparators.PageRankComperator;
import enums.CommandType;
import scala.Tuple2;
import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.PageRankResults;
import univ.bigdata.course.providers.MovieRecommands;

public class MainRunner {

    public static void main(String[] args) {
    	MoviesStorage moviesStorage = null;
    	//    	CommandType query = CommandType.fromString(args[0]);
    	CommandType query = CommandType.fromString("map");
    	
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
			
			
			//String RecommendationFileName = args[1];
			String Movies_train = "train.txt";
			String Movies_test	= "test.txt";
			
			
//			try(BufferedReader br = new BufferedReader(new FileReader(Movies_train))) {
			    try{
//			    PrintStream printer  = new PrintStream (new File(outputPath));
			    
		    	//initialize movie storage and spark context
		    	MovieRecommands mr = new MovieRecommands(Movies_train,Movies_test);
//		    	mr.recommend(Movies_train);//.forEach(a->.println(a.printRecommendations()));
		    	Double ans = mr.getMAPValue();
			}catch (Exception e) {
				e.printStackTrace();
				throw new IllegalArgumentException("Program Second argument is illegal.");
			}finally {
				if(moviesStorage != null){
					moviesStorage.closeJavaSparkContext();
				}
			} 
			
			
			break;
			
		case PAGE_RANK:

//			String movieSimpleFile = args[1];
			
			String movieSimpleFile = "movies-simple.txt";
	    	//initialize movie storage and spark context
	    	moviesStorage = new MoviesStorage(movieSimpleFile);
	    	
			JavaRDD<String> Edges = moviesStorage.getPairedUser();

			//TODO: Need to insert Carmi Code to generate JavaRDD<String>
			try {
				List<PageRankResults> unmodifiablePRresults = JavaPageRank.Rank(Edges, 100);
				List<PageRankResults> modifiablePRresults = new ArrayList<PageRankResults>(unmodifiablePRresults);
				modifiablePRresults.sort(new PageRankComperator());
				
				if(modifiablePRresults.size() > 100){
					modifiablePRresults = modifiablePRresults.subList(0, 99);
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
