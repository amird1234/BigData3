package univ.bigdata.course;

import java.io.BufferedReader;
import java.io.FileReader;

import enums.CommandType;

public class MainRunner {

    public static void main(String[] args) {
    	MoviesStorage moviesStorage = null;
    	long moviesNum = 0;
    		
    	CommandType query = CommandType.fromString(args[0]);
    	
    	switch (query) {
		case COMMANDS:
			String commandsFileName = args[1];
			try(BufferedReader br = new BufferedReader(new FileReader(commandsFileName))) {
			    StringBuilder sb = new StringBuilder();
			    String inputPath = br.readLine();
			    String outputPath = br.readLine();
			    
		    	//initialize movie storage and spark context
		    	moviesStorage = new MoviesStorage(inputPath);
		    	
		    	moviesStorage.startQueryRunner(outputPath);

			}catch (Exception e) {
				throw new IllegalArgumentException("Program Second argument is illegal.");
			}finally {
				if(moviesStorage != null){
					moviesStorage.closeJavaSparkContext();
				}
			} 
			
			break;
		case RECOMMEND:
			
			break;
			
		case MAP_COMMAND:
			
			break;
			
		case PAGE_RANK:

			String inputPath = null;
	    	//initialize movie storage and spark context
	    	moviesStorage = new MoviesStorage(inputPath);
	    	
	    	
	    	moviesStorage.closeJavaSparkContext();
			break;
		default:
			throw new IllegalArgumentException("Ptogram First argument is illegal, needs to be commands/recommend/mappagerank");
		
		}
    	
    	String inputPath = args[0];
    	//initialize movie storage and spark context
    	moviesStorage = new MoviesStorage(inputPath);
        
    	//call first implemented function
        moviesNum = moviesStorage.moviesCount();
        System.out.println("number of movies " + moviesNum);
        Double total = moviesStorage.totalMoviesAverageScore();
        System.out.println("total Movies Average Score " + total);
        System.out.println("total Movies B00004CK40 Average Score " + moviesStorage.totalMovieAverage("B00004CK40"));
    }
}
