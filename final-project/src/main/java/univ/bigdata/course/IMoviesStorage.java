/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course;

import univ.bigdata.course.compare.compareClass;
import univ.bigdata.course.compare.compareD;
import univ.bigdata.course.movie.Movie;

import java.util.List;
import java.util.Map;

public interface IMoviesStorage {

    /**
     * Method which calculates total scores average for all movies.
     *
     * @return - the average
     */
    double totalMoviesAverageScore();

    /**
     * For given movies calculates an average score of this movie.
     *
     * @param productId - id of the movie to calculate the average score.
     * @return - movie's average
     */
    double totalMovieAverage(final String productId);

    /**
     * For each movie calculates it's average score. List should be sorted
     * by average score in decreasing order and in case of same average tie
     * should be broken by natural order of product id. Only top k movies
     * with highest average should be returned
     *
     * @param topK - number of top movies to return
     * @return - list of movies where each @{@link Movie} includes it's average
     */
    List<Movie> getTopKMoviesAverage(final long topK);

    /**
     * Finds movie with the highest average score among all available movies.
     * If more than one movie has same average score, then movie with lowest
     * product id ordered lexicographically.
     *
     * @return - the movies record @{@link Movie}
     */
    Movie movieWithHighestAverage();

    /**
     * @return - the product id of most reviewed movie among all movies
     */
    String mostReviewedProduct();

    /**
     * Computes reviews count per movie, sorted by reviews count in decreasing order, for movies
     * with same amount of review should be ordered by product id. Returns only top k movies with
     * highest review count.
     *
     * @return - returns map with movies product id and the count of over all reviews assigned to it.
     */
    List<compareClass> reviewCountPerMovieTopKMovies(final int topK);

    /**
     * Computes most popular movie which has been reviewed by at least
     * numOfUsers (provided as parameter).
     *
     * @param numOfUsers - limit of minimum users which reviewed the movie
     * @return - movie which got highest count of reviews
     */
    String mostPopularMovieReviewedByKUsers(final int numOfUsers);

    /**
     * Compute map of words count for top K words
     *
     * @param topK - top k number
     * @return - map where key is the word and value is the number of occurrences
     * of this word in the reviews summary, map ordered by words count in decreasing order.
     */
    List<compareClass> moviesReviewWordsCount(final int topK);

    /**
     * Compute words count map for top Y most reviewed movies. Map includes top K
     * words.
     *
     * @param topMovies - number of top review movies
     * @param topWords - number of top words to return
     * @return - map of words to count, ordered by count in decreasing order.
     */
    List<compareClass> topYMoviewsReviewTopXWordsCount(final int topMovies, final int topWords);

    /**
     * Compute top k most helpful users that provided reviews for movies
     *
     * @param k - number of users to return
     * @return - map of users to number of reviews they made. Map ordered by number of reviews
     * in decreasing order.
     */
    List<compareD> topKHelpfullUsers(final int k);

    /**
     * Total movies count
     */
    long moviesCount();

}
