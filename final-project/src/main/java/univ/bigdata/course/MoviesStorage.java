/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course;

import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieForAverage;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.providers.MoviesProvider;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

import comparators.MovieReviewComparator;
import comparators.MovieScoreComparator;
import univ.bigdata.course.movie.User;

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
	
    public MoviesStorage(final MoviesProvider provider) {
        //TODO: read movies using provider interface
       this.localProvider = provider;
    }

    @Override
    public double totalMoviesAverageScore() {
    	Double Average = 0.0;
    	Integer NumOfReviewForMovie = 0;
    	
    	//Sum all the scores for all the movies
        while(localProvider.hasMovie()){
        	MovieReview mr = localProvider.getMovie();
        	if(mr != null){
        		Average += mr.getMovie().getScore();
        		NumOfReviewForMovie++;
        	}
        }
        if(NumOfReviewForMovie == 0){
        	return 0;
        }
        Average = Average/NumOfReviewForMovie;
        return Average;
    }

    @Override
    public double totalMovieAverage(String productId) {
    	Double Average = 0.0;
    	Integer NumOfReviewForMovie = 0;
    	if(productId == null){
    		throw new RuntimeException();
    	}
    	
    	//Sum all the scores for the specific movie
        while(localProvider.hasMovie()){
        	MovieReview mr = localProvider.getMovie();
        	if(mr != null){
        		if(mr.getMovie().getProductId().equals(productId)){
        			Average += mr.getMovie().getScore();
        			NumOfReviewForMovie++;
        		}
        	}
        }
        if(NumOfReviewForMovie == 0){
        	throw new RuntimeException();
        }
        Average = Average/NumOfReviewForMovie;
        return Average;
    }

    @Override
    public List<Movie> getTopKMoviesAverage(long topK) {
    	int i;
    	List<Movie> returnedList = new ArrayList<>();
    	List<Movie> tempMovieList = new ArrayList<>();
    	if((topK<0)){
    		throw new RuntimeException();
    	}
    	tempMovieList = getMoviesAverage();
    	tempMovieList.sort(new MovieScoreComparator());
    	for(i=0 ; (i<topK) && (i<tempMovieList.size()) ; i++){
    		returnedList.add(tempMovieList.get(i));
    	}
    	return returnedList;
    }

    @Override
    public Movie movieWithHighestAverage() {
        List<Movie> movArray = getTopKMoviesAverage(1);
        if(movArray.size()>0){
        	return movArray.get(0);
        }else{
        	return null;
        }
    }

    @Override
    public List<Movie> getMoviesPercentile(double percentile) {
    	List<Movie> totalList;
    	List<Movie> returnedList;
    	int totalNum,i,perNum;
    	
    	if(percentile < 0 || percentile > 100)
    		throw new RuntimeException();
    	
    	returnedList = new ArrayList<>();
    	totalList = getMoviesAverage();
    	if(!totalList.isEmpty())
    	{
    		totalList.sort(new MovieScoreComparator());
    		totalNum = totalList.size();
    		perNum = totalNum - (int)((percentile/100)*totalNum);
    		for(i=0; i < perNum; i++)
    		{
    			returnedList.add(totalList.get(i));
    		}
    	}
    	return returnedList;
    }

    @Override
    public String mostReviewedProduct() {
        Map<String, Long> tempMap = reviewCountPerMovieTopKMovies(1);
        String returnedString = null;
        for(String s : tempMap.keySet()){
        	returnedString = s;
        }
        return returnedString;
    }

    @Override
    public Map<String, Long> reviewCountPerMovieTopKMovies(int topK) {
    	List<MovieForAverage> movieList = new ArrayList<>();
    	HashMap<String, MovieForAverage> aveMap = new HashMap<>();
    	Map<String, Long> returnedMap = new LinkedHashMap<>();
    	int i;
    	if((topK<0)){
    		throw new RuntimeException();
    	}
    	 while(localProvider.hasMovie()){
         	MovieReview mr = localProvider.getMovie();
         	if(mr != null){
         		MovieForAverage movForAve = aveMap.get(mr.getMovie().getProductId());
         		//if movie already exist in hash map.
         		if(movForAve != null){
         			//update movie number of reviews.
         			movForAve.incNumOfReviews();
         		}else{
         			//create a new MovieForAverage instance and add it to HashMap.
         			movForAve = new MovieForAverage(mr.getMovie().getProductId(), mr.getMovie().getScore());
         			movForAve.incNumOfReviews();
         			aveMap.put(mr.getMovie().getProductId(), movForAve);
         		}
         	}
         }
    	 //Goes over all the movies, calculates their average and adds them to the returned list.
    	 for (MovieForAverage value : aveMap.values()) {
//    		    MovieForAverage tempM = new MovieForAverage(value.getProductId(), value.getAverage());
    		    movieList.add(value);
    	 }
    	 movieList.sort(new MovieReviewComparator());
    	 for(i=0 ; (i<topK) && (i<movieList.size()) ; i++){
    		 MovieForAverage tempMFA = movieList.get(i);
    		 returnedMap.put(tempMFA.getProductId(), (new Long(tempMFA.getNumOfReviews())));
    	 }
    	 
    	return returnedMap; 
    }

    @Override
    public String mostPopularMovieReviewedByKUsers(int numOfUsers) {
    	List<Movie> movieList = getMoviesAverage();
    	List<Movie> movieListFinal = new ArrayList<>();
    	int total = movieList.size(), i;
    	if(numOfUsers < 0)
    		throw new RuntimeException();
    	//all movies
    	Map<String, Long> movieRCount = reviewCountPerMovieTopKMovies(total);
    	//only movies with at least numOfUsers reviews.
    	if(!movieRCount.isEmpty())
    	{
    		Map<String, Long> movienumOfUsersCount = new LinkedHashMap<>();
    		Iterator<Entry<String, Long>> entries = movieRCount.entrySet().iterator();
    		while (entries.hasNext()) 
    		{
    			Map.Entry<String, Long> entry = (Map.Entry<String, Long>) entries.next();
    			if(entry.getValue() >= numOfUsers)
    			{
    				movienumOfUsersCount.put(entry.getKey(), entry.getValue());
    			}
    		}
    		//find movie with max average
    		if(!movienumOfUsersCount.isEmpty())
    		{
    			for(i=0 ; i<total; i++)
    			{
    				if(movienumOfUsersCount.containsKey(movieList.get(i).getProductId()))
    				{
    					movieListFinal.add(movieList.get(i));
    				}
    			}
    		}
    		else
    		{
    			return null;
    		}
    	}
    	else
    	{
    		return null;
    	}
    	if(!movieListFinal.isEmpty())
    	{
    		movieListFinal.sort(new MovieScoreComparator());
    		return movieListFinal.get(0).getProductId(); 
    	}
    	return null;
    }

    @Override
    public Map<String, Long> moviesReviewWordsCount(int topK) {
    	//count movies
    	long movies =  moviesCount();
    	//Compute words count map for all the movies. Map includes top K words.
    	return topYMoviewsReviewTopXWordsCountLong(movies,topK);
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
    	Map<String, Long> tmpTopYMoviewsReviewWordsCount = new LinkedHashMap<String, Long>();
    	Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
    	List<Movie> tempMovieList = new ArrayList<>();
    	List<String> tempTopKMovieNameList = new ArrayList<>();										
    	tempMovieList = getMoviesAverage();
    	tempMovieList.sort(new MovieScoreComparator());
    	long n = topMovies;
    	if(tempMovieList.size()<topMovies){
    		n = tempMovieList.size();
    	}
    	for(int i=0 ; i<n ; i++){
    		tempTopKMovieNameList.add(tempMovieList.get(i).getProductId());
    	}
    	while (localProvider.hasMovie()) {
			MovieReview mr = localProvider.getMovie();
			String currentName = mr.getMovie().getProductId();
			if(tempTopKMovieNameList.contains(currentName)){
				//we want to count the words only in top y movies
				String[] reviewTokens = mr.getReview().split(" ");
				for (String token: reviewTokens) {
					if (tmpTopYMoviewsReviewWordsCount.containsKey(token)) {
						Long count = tmpTopYMoviewsReviewWordsCount.get(token) +1;
						tmpTopYMoviewsReviewWordsCount.remove(token);
						tmpTopYMoviewsReviewWordsCount.put(token, count);
					} else {
						tmpTopYMoviewsReviewWordsCount.put(token, (long) 1);
					}
				}
			}
		}
		List<Map.Entry<String, Long>> list = 
				new LinkedList<Map.Entry<String, Long>>(tmpTopYMoviewsReviewWordsCount.entrySet());
		// Sort list by key
		Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
			public int compare(Map.Entry<String, Long> o1,
                                           Map.Entry<String, Long> o2) {
				return (o1.getKey()).compareTo(o2.getKey());
			}
		});
		Collections.reverse(list);
			// Sort list by value
			Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
				public int compare(Map.Entry<String, Long> o1,
	                                           Map.Entry<String, Long> o2) {
					return (o1.getValue()).compareTo(o2.getValue());
				}
			});
			tmpTopYMoviewsReviewWordsCount.clear();
			//reverse the sorted list
			Collections.reverse(list);
			// Convert sorted map back to a Map
			for (Iterator<Map.Entry<String, Long>> it = list.iterator(); it.hasNext();) {
				Map.Entry<String, Long> entry = it.next();
				sortedMap.put(entry.getKey(), entry.getValue());
			}
    			int k = 0;
    			for (Map.Entry<String, Long> entry : sortedMap.entrySet()) {
    				//insert the top k words
    				if(k<topWords){
    					tmpTopYMoviewsReviewWordsCount.put(entry.getKey(), entry.getValue());
    					k++;
    				}
    			}
    	
        return tmpTopYMoviewsReviewWordsCount;
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
		Map<String, User> tmpTopKHelpfullUsers = new LinkedHashMap<String, User>();
		Map<String, Double> topKHelpfullUsersRetVal = new LinkedHashMap<String, Double>();
		List<User> usersList = new ArrayList<>();
		String[] helpfullness = null;
		Integer numerator = 0, denominator = 0; 

		// go over all movies and add all users one by one.
		while (localProvider.hasMovie()) {
			MovieReview mr = localProvider.getMovie();
			if (mr != null) {
				// parse helpfulness into to strings, "/" separated
				helpfullness = mr.getHelpfulness().split("/");
				// parse string into an integer
				numerator = Integer.parseInt(helpfullness[0]);
				denominator = Integer.parseInt(helpfullness[1]);;

				if (tmpTopKHelpfullUsers.containsKey(mr.getUserId())) {
					// if user already exists in hash map, get a reference.
					User user = tmpTopKHelpfullUsers.get(mr.getUserId());
					user.incNumeratorDeniminator(numerator, denominator); //add helpful/total #reviews
					user.incNumOfReviews(); // another review by an existing user, increment counter
				} else {
					// first time we encounter this user, let's add it!
					User user = new User(mr.getUserId(), numerator, denominator, 1);
					tmpTopKHelpfullUsers.put(mr.getUserId(), user);
				}
			}
		}

		// move all users to a list so we'll be able to sort them
		for (User user : tmpTopKHelpfullUsers.values()) {
			usersList.add(user);
		}

		usersList.removeIf(p -> p.isHelpfull() == false); 
		// sort the reviewers by helpfulness, number of reviews, userID
		usersList.sort(new Comparator<User>() {
			public int compare(User o1, User o2) {
				Integer delta = o2.getHelpfullness().compareTo(o1.getHelpfullness());
				if (delta == 0) {
					return o1.getUserID().compareTo(o2.getUserID());	
				} else {
					return ((delta > 0) ? 1 : -1);
				}
			}
		});

		// reduce list to only top K reviewers
		usersList = usersList.subList(0, java.lang.Math.min(k, usersList.size()));

		// translate List into a Map
		for (User user : usersList) {
			topKHelpfullUsersRetVal.put(user.getUserID(), user.getHelpfullness());
		}

		return topKHelpfullUsersRetVal;
    }

    @Override
    public long moviesCount() {
    	long numOfMovies = 0;
    	List<String> moviesID = new ArrayList<>();
    	while(localProvider.hasMovie()){
         	MovieReview mr = localProvider.getMovie();
         	if(!moviesID.contains(mr.getMovie().getProductId())){
         		moviesID.add(mr.getMovie().getProductId());
         		numOfMovies++;
         	}
         }
    	return numOfMovies; 
    }
    
    /***
     * This function calculates the average score for the movies in the provider
     * @return A list of Movie objects each holding its average score in the score variable. 
     */
    private List<Movie> getMoviesAverage(){
    	List<Movie> returnedList = new ArrayList<>();
    	HashMap<String, MovieForAverage> aveMap = new HashMap<>();
    	 while(localProvider.hasMovie()){
         	MovieReview mr = localProvider.getMovie();
         	if(mr != null){
         		MovieForAverage movForAve = aveMap.get(mr.getMovie().getProductId());
         		//if movie already exist in hash map.
         		if(movForAve != null){
         			//update movie score sum and number of reviews.
         			movForAve.addToMovieScore(mr.getMovie().getScore());
         			movForAve.incNumOfReviews();
         		}else{
         			//create a new MovieForAverage instance and add it to HashMap.
         			movForAve = new MovieForAverage(mr.getMovie().getProductId(), mr.getMovie().getScore());
         			movForAve.incNumOfReviews();
         			aveMap.put(mr.getMovie().getProductId(), movForAve);
         		}
         	}
         }
    	 //Goes over all the movies, calculates their average and adds them to the returned list.
    	 for (MovieForAverage value : aveMap.values()) {
    		    Movie tempM = new Movie(value.getProductId(), Double.parseDouble(new DecimalFormat("##.#####").format(value.getAverage())));
    		    returnedList.add(tempM);
    	 }
    	return returnedList; 
    }
}
