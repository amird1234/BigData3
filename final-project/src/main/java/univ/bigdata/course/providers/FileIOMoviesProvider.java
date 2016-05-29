/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.providers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieReview;

public class FileIOMoviesProvider implements MoviesProvider {
	
	private ArrayList<MovieReview> reviews 				= null;
	
	public static final String proIDPrefix 		= "product/productId:";
	public static final String userIDPrefix 	= "review/userId:";
	public static final String profNamePrefix 	= "review/profileName:";
	public static final String helpPrefix		= "review/helpfulness:";
	public static final String scorePrefix 		= "review/score:";
	public static final String timePrefix 		= "review/time:";
	public static final String summeryPrefix 	= "review/summary:";
	public static final String textPrefix 		= "review/text:";
	
	private int reviewIndex = 0;
	
	
	/**
	 * this function will parse inFile (the path to input) and will:
	 * - for each (new) movie - parse productID, create a Movie instance.
	 * - for each review - parse all attributes, create a MovieReview instance (with all att's), insert to reviews ArrayList. 
	 * @param inFile - path to the input file containing the MovieReviews.
	 */
	public FileIOMoviesProvider(String inFile) {
		
		reviews = new ArrayList<>();
		
		URL url = this.getClass().getClassLoader().getResource ( inFile );
		
		String productId,userId,profileName,helpfulness,summary,review;
		Date timestamp;
		Double score;
		
		//open the input file.
		try (BufferedReader br = new BufferedReader(new FileReader(url.getPath()))) {	
		    String line;
		    //Read all lines from input text file, one by one.
		    while ((line = br.readLine()) != null) {
		    	
		       //create Movie
		       productId 		= line.substring((line.indexOf(proIDPrefix)+ proIDPrefix.length()+ 1), line.indexOf(userIDPrefix)-1);
		       score 			= Double.valueOf(line.substring((line.indexOf(scorePrefix)+ scorePrefix.length()+1),(line.indexOf(timePrefix)-1)));
		       Movie tempMovie 	= new Movie(productId, score);
		       
		       //create movie review
		       userId 			= line.substring((line.indexOf(userIDPrefix)+userIDPrefix.length() + 1),line.indexOf(profNamePrefix)-1);
		       profileName 		= line.substring((line.indexOf(profNamePrefix)+profNamePrefix.length() + 1),line.indexOf(helpPrefix)-1);
		       helpfulness 		= line.substring((line.indexOf(helpPrefix)+helpPrefix.length() + 1),line.indexOf(scorePrefix)-1);
		       summary			= line.substring((line.indexOf(summeryPrefix)+summeryPrefix.length() + 1),line.indexOf(textPrefix)-1);
		       review			= line.substring(line.indexOf(textPrefix)+ textPrefix.length() + 1);
		       timestamp 		= new Date(Long.valueOf(line.substring((line.indexOf(timePrefix) + timePrefix.length() +1),line.indexOf(summeryPrefix) -1)));
		       
		       //create a new MovieReview instance according to the parsed line.
		       MovieReview tempMovieReview = new MovieReview(tempMovie, userId, profileName, helpfulness, timestamp, summary, review);
		       
		       //insert the new MovieReview to the ArrayList
		       reviews.add(tempMovieReview);
		       
		       
		    } 
		}catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
    @Override
    public boolean hasMovie() {
    	if(reviewIndex < reviews.size()){
    		return true;
    	}else{
    		reviewIndex=0;
    		return false;
    	}
    }

    @Override
    public MovieReview getMovie() {
    	if(reviewIndex <= reviews.size()){
    		return reviews.get(reviewIndex++);
    	}else{
    		return null;
    	}
    }
}
