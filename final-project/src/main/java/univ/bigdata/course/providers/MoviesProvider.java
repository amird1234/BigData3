/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.providers;

import univ.bigdata.course.movie.MovieReview;

public interface MoviesProvider {

    boolean hasMovie();

    MovieReview getMovie();
}
