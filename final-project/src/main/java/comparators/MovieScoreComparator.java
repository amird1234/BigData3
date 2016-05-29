/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package comparators;

import java.util.Comparator;

import univ.bigdata.course.movie.Movie;

/**
 * This class is used for comparing two Movies by their score.
 * @author Avner
 *
 */
public class MovieScoreComparator implements Comparator<Movie>{

	@Override
	public int compare(Movie m1, Movie m2) {
		if(((Double)m2.getScore()).equals(m1.getScore())){
			return m1.getProductId().compareTo(m2.getProductId());
		}else{
			return ((Double)m2.getScore()).compareTo(m1.getScore()); 
		}
	}

}
