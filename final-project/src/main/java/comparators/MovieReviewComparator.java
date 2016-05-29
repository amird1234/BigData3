/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package comparators;

import java.util.Comparator;

import univ.bigdata.course.movie.MovieForAverage;

public class MovieReviewComparator implements Comparator<MovieForAverage>{

	@Override
	public int compare(MovieForAverage m1, MovieForAverage m2) {
		if((m2.getNumOfReviews()).equals(m1.getNumOfReviews())){
			return m1.getProductId().compareTo(m2.getProductId());
		}else{
			return m2.getNumOfReviews().compareTo(m1.getNumOfReviews()); 
		}
	}

}
