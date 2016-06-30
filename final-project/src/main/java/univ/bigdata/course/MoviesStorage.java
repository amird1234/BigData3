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
import univ.bigdata.course.compare.compareClass;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.movie.User;
import univ.bigdata.course.providers.MoviesProvider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import comparators.MovieScoreComparator;
import scala.Serializable;
import scala.Tuple2;

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
public class MoviesStorage implements IMoviesStorage,Serializable {
	MoviesProvider localProvider;
	JavaRDD<MovieReview> movieReviews;
	JavaSparkContext sc;
	PrintStream outFile = null;
	PrintStream printer;
	
	/**
	 * constructor
	 * initializes the spark context
	 * 
	 * @param path - a path to file
	 */
    public MoviesStorage(String path) {
		sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("mySparkApp"));
		JavaRDD<String> fileLines = sc.textFile(path);
		movieReviews = fileLines.map(MovieReview::new);
		
    }

	@Override
	public double totalMoviesAverageScore() {
    	Double Average;
    	JavaRDD<Double> moviesScore = movieReviews.map(movie -> movie.getMovie().getScore());
    	
    	Average =  moviesScore.reduce((x1,x2) -> x1+x2);
    	return Math.round((Average/moviesScore.count())*100000)/100000.0d;
	}

    @Override
    public double totalMovieAverage(String productId) {
    	JavaRDD<Double> movies = movieReviews.filter(movie -> movie.getMovie().getProductId().contains(productId)).map(movie -> movie.getMovie().getScore());
    	if(movies.isEmpty()){
    		return 0;
    	}
        double Average = movies.reduce((x1,x2) -> x1+x2);
        return Math.round((Average/movies.count())*100000)/100000.0d;
    }

    @Override
    public List<Movie> getTopKMoviesAverage(long topK) {
    	JavaPairRDD<Movie,Double> list = movieReviews.mapToPair(s->new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(),1)))
            	.reduceByKey((x1, x2)-> new Tuple2<>(x1._1 + x2._1, x1._2 + x2._2)).map(s -> new Movie(s._1, (s._2._1 / s._2._2))).mapToPair(s->new Tuple2<>(s, s.getScore())).sortByKey();

    	return list.map(s -> new Movie(s._1.getProductId(),s._2 )).top((int)topK);
    }

    @Override
    public Movie movieWithHighestAverage() {
    	return getTopKMoviesAverage(1).get(0);
    }

    @Override
    public String mostReviewedProduct() {
    	 Map<String,Long> temp = reviewCountPerMovieTopKMovies(1);
    		if(!temp.isEmpty())
    		{
    			Map.Entry<String,Long> entry=temp.entrySet().iterator().next();
    			return entry.getKey();
    		}
    		else
    			return null;
    }

    
	@Override
    public Map<String, Long> reviewCountPerMovieTopKMovies(int topK) {
    	if(topK < 0)
        	throw new RuntimeException();
    		
    	JavaPairRDD<compareClass, Long> tempReviewCount = movieReviews.mapToPair(s -> new Tuple2<>(s.getMovie().getProductId(),Long.valueOf(1)))
                    .reduceByKey((a, b) -> a + b).map(s -> new compareClass(s._2, s._1)).mapToPair(s->new Tuple2<>(s, s.getFirstKey())).sortByKey();;
        Map<String, Long> ret= new java.util.LinkedHashMap<String,Long>() ;
        tempReviewCount.take((int)topK).forEach(item->ret.put(item._1.getSecoundKey(),item._2));
        return ret;
    }

    @Override
    public String mostPopularMovieReviewedByKUsers(int numOfUsers) {
    	if(numOfUsers < 0)
    		throw new RuntimeException();
    	
    	List<Movie> popularMovie =  movieReviews
                .mapToPair(s-> new Tuple2<>(s.getMovie().getProductId(), new Tuple2<>(s.getMovie().getScore(), 1)))
                .reduceByKey((x1, x2)-> new Tuple2<>(x1._1 + x2._1, x1._2 + x2._2))
                .filter(s -> s._2._2 >= numOfUsers).map(s -> new Movie(s._1, s._2._1)).collect();
    	
    	if(!popularMovie.isEmpty())
    	{
    		popularMovie.sort(new MovieScoreComparator());
    		return popularMovie.get(0).getProductId();
    	}
    	else
    	{
    		return null;
    	}
    }

   
	@Override
    public Map<String, Long> moviesReviewWordsCount(int topK) {
		
		return topYMoviewsReviewTopXWordsCount((int)moviesCount(),topK);
    }
    
    @Override
    public Map<String, Long> topYMoviewsReviewTopXWordsCount(int topMovies, int topWords) {
    	Map<String,Long> topYMovies = reviewCountPerMovieTopKMovies(topMovies);
    	//top k movies and their reviews
    	JavaPairRDD<compareClass,Long> count = movieReviews.filter(s-> topYMovies.containsKey(s.getMovie().getProductId()))
    			.map(s-> s.getReview()).flatMap(s-> Arrays.asList(s.split(" "))).mapToPair(s->new Tuple2<>(s, Long.valueOf(1)))
    			.reduceByKey((x1,x2)->x1+x2).map(s->new compareClass(s._2,s._1)).mapToPair(s->new Tuple2<>(s, s.getFirstKey())).sortByKey();
    	
        Map<String, Long> ret= new java.util.LinkedHashMap<String,Long>() ;
        count.take((int)topWords).forEach(item->ret.put(item._1.getSecoundKey(),item._2));
        return ret;
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
        if(k < 0)
        	throw new RuntimeException();

        Map<String, Double> ret = new HashMap<String, Double>();
        JavaPairRDD<User,Double> usersHelpful =  movieReviews
                    .mapToPair(a -> new Tuple2<>(a.getUserId(), a.getHelpfulness()))
                    .mapToPair(s -> new Tuple2<>(s._1, new Tuple2<>(Double.parseDouble(s._2.substring(0,s._2.lastIndexOf('/'))),
                            Double.parseDouble(s._2.substring(s._2.lastIndexOf('/') + 1, s._2.length())))))
                    .filter(s -> s._2._2 != 0)
                    .reduceByKey((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                    .map(s -> new User(s._1, s._2._1/s._2._2)).mapToPair(s-> new Tuple2<>(s, s.getHelpfullness())).sortByKey();

        // reduce list to only top K reviewers
         usersHelpful.take(k).forEach(item->ret.put(item._1.getUserID(),item._1.getHelpfullness()));
         return ret;
        // translate List into a Map
   /*     for (User user : usersList) {
        	topKHelpfullUsersRetVal.put(user.getUserID(), user.getHelpfullness());
        }

        return topKHelpfullUsersRetVal;*/
    	//throw new UnsupportedOperationException("You have to implement this method on your own.");
    }

    @Override
    public long moviesCount() {
		JavaRDD<String> moviesRDD = movieReviews.map(movie -> movie.getMovie().getProductId()).distinct();
		return moviesRDD.count();
    }
    public void closeJavaSparkContext(){
    	sc.close();
    }
    
    public void startQueryRunner(String outputFile){
		try {
			outFile = new PrintStream (new File(outputFile)); //comment
//		outFile = new PrintStream (new File("queries_out.txt")); // not comment
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        	
        printer = outFile;
    }
    
    public void runQuery(String contant){
    	if(contant == null){
    		return;
    	}
    	String[] variables = contant.split(" ");
    	String query = variables[0];
    	
    	switch (query) {
		case "totalMoviesAverageScore":
			printer.println("Total average: " + totalMoviesAverageScore());
			break;
			
		case "totalMovieAverage":
			printer.println(totalMovieAverage(variables[1]));
			break;
			
		case "getTopKMoviesAverage":
			long K1 = Long.parseLong(variables[1]);
			getTopKMoviesAverage(K1).stream().forEach(printer::println);
			break;
			
		case "movieWithHighestAverage":
			printer.println("The movie with highest average:  " + movieWithHighestAverage());
			break;
			
		case "mostReviewedProduct":
			printer.println("The most reviewed movie product id is " + mostReviewedProduct());
			break;
			
		case "reviewCountPerMovieTopKMovies":
			int K2 = Integer.parseInt(variables[1]);
			reviewCountPerMovieTopKMovies(K2)
            .entrySet()
            .stream()
            .forEach(pair -> printer.println("Movie product id = [" + pair.getKey() + "], reviews count [" + pair.getValue() + "]."));
			break;
			
		case "mostPopularMovieReviewedByKUsers":
			int K3 = Integer.parseInt(variables[1]);
			printer.println("Most popular movie with highest average score, reviewed by at least " + variables[1] + "users " + mostPopularMovieReviewedByKUsers(K3));
			break;
			
		case "topYMoviewsReviewTopXWordsCount":
			int K4 = Integer.parseInt(variables[1]);
			int K5 = Integer.parseInt(variables[2]);
			topYMoviewsReviewTopXWordsCount(K4, K5)
            .entrySet()
            .forEach(pair -> printer.println("Word = [" + pair.getKey() + "], number of occurrences [" + pair.getValue() + "]."));
			break;
			
		case "topKHelpfullUsers":
			int K6 = Integer.parseInt(variables[1]);
			topKHelpfullUsers(K6)
            .entrySet()
            .forEach(pair -> printer.println("User id = [" + pair.getKey() + "], helpfulness [" + pair.getValue() + "]."));
			break;

		default:
			break;
		}
    }
    public  JavaRDD<String> getPairedUser(){
    	//users and movies list
    	JavaPairRDD<String, String> UsersMovies = movieReviews.mapToPair(s->new Tuple2<>(s.getMovie().getProductId(), s.getUserId())).distinct();
    	//user and user list
    	JavaRDD<String> UserUser = UsersMovies.join(UsersMovies).filter(s->((s._2._1.compareTo(s._2._2))!=0)).map(s->s._2._1 + " " + s._2._2).distinct();
    	return UserUser;
        /*JavaRDD<String> UserUser = UsersMovies.join(UsersMovies).filter(s->((s._2._1.compareTo(s._2._2))<0)).map(s->s._2._1 + " " + s._2._2).distinct();
        JavaRDD<String> UserUser2 = UsersMovies.join(UsersMovies).filter(s->((s._2._1.compareTo(s._2._2))>0)).map(s->s._2._2 + " " + s._2._1).distinct();
        return UserUser.union(UserUser2).distinct();*/
    }

}
