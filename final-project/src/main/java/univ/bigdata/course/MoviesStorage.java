/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course;
import org.apache.spark.api.java.*;
import org.apache.spark.*;

import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.providers.MoviesProvider;

import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.Function2;

import scala.Enumeration.Val;
import scala.collection.mutable.LinkedHashMap;

/**
 * Main class which capable to keep all information regarding movies review.
 * Has to implements all methods from @{@link IMoviesStorage} interface.
 * Also presents functionality to answer different user queries, such as:
 * <p>
 * 1. Total number of distinct movies reviewed.
 * 2. Total number of distinct users that produces the review.
 * 3. Average review score for all movies.
 * 4. Average review score per single movie.
 * 5. Most popular movie reviewed by at least "K" unique users
 * 6. Word count for movie review, select top "K" words
 * 7. K most helpful users
 */
public class MoviesStorage implements IMoviesStorage {
	MoviesProvider localProvider;
	JavaRDD<MovieReview> movieReviews;
	
	/**
	 * constructor
	 * initializes the spark context
	 * 
	 * @param path - a path to file
	 */
    public MoviesStorage(String path) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("mySparkApp"));
		JavaRDD<String> fileLines = sc.textFile(path);
		movieReviews = fileLines.map(MovieReview::new);
    }

	@SuppressWarnings("serial")
	@Override
	public double totalMoviesAverageScore() {
    	Double Average;
    	JavaRDD<Double> moviesScore = movieReviews.map(movie -> movie.getMovie().getScore());
    	
    	Average =  moviesScore.reduce((x1,x2) -> x1+x2);
    	return Average/moviesScore.count();
		//throw new UnsupportedOperationException("You have to implement this method on your own.");
	}

    @Override
    public double totalMovieAverage(String productId) {
    	JavaRDD<MovieReview> movies = movieReviews.filter(movie -> movie.getMovie().getProductId().contains(productId));
        JavaRDD<Double> score = movies.map(movie -> movie.getMovie().getScore());
        double Average = score.reduce((x1,x2) -> x1+x2);
        return Average/score.count();
    	//throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public List<Movie> getTopKMoviesAverage(long topK) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public Movie movieWithHighestAverage() {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public List<Movie> getMoviesPercentile(double percentile) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public String mostReviewedProduct() {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public Map<String, Long> reviewCountPerMovieTopKMovies(int topK) {
    	throw new UnsupportedOperationException("You have to implement this method on your own."); 
    }

    @Override
    public String mostPopularMovieReviewedByKUsers(int numOfUsers) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

   
	@Override
    public Map<String, Long> moviesReviewWordsCount(int topK) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }
    /**
     * Help function get long topMovies instead of int.
     * and compute map that define at topYMoviewsReviewTopXWordsCount
     * (Compute words count map for top Y most reviewed movies. Map includes top K words.)
     *
     * @param topMovies - number of top review movies (long)
     * @param topWords - number of top words to return
     * @return - map of words to count, ordered by count in decreasing order.
     */
    public Map<String, Long> topYMoviewsReviewTopXWordsCountLong(long topMovies, int topWords) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }
    
    @Override
    public Map<String, Long> topYMoviewsReviewTopXWordsCount(int topMovies, int topWords) {
        return topYMoviewsReviewTopXWordsCountLong((long)topMovies, topWords);
    }

    @Override
    /**
     * Will calculate the top K helpful reviewers.
     * top K helpful reviewers will be calculated using 2 keys:
     * 1) helpfulness (sum of numerators / sum of denominators)
     * 2) numOfReviews (of the user)
     * 3) Lexicographically by userID.
     * 
     * Will return a Map (userID->numOfReviews), this map will be sorted using 2 keys:
     * 1) helpfulness
     * 2) numOfReviews (of the user)
     * 3) Lexicographically by userID.
     * 
     * @param k
     * @return
     */
    public Map<String, Double> topKHelpfullUsers(int k) {
    	throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public long moviesCount() {
		JavaRDD<String> moviesRDD = movieReviews.map(movie -> movie.getMovie().getProductId()).distinct();
		return moviesRDD.count();
    }
}
