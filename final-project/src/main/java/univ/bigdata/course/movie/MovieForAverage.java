/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

public class MovieForAverage extends Movie{
	private Integer numOfReviews;
	
	public MovieForAverage(String productId, double score){
		super(productId,score);
		numOfReviews = 0;
	}
	
	public MovieForAverage(String productId, double score, Integer nor){
		super(productId,score);
		numOfReviews = nor;
	}
	public void incNumOfReviews(){
		numOfReviews++;
	}
	
	public Integer getNumOfReviews(){
		return numOfReviews;
	}
	
	public void  addToMovieScore(double addedScore){
		super.setScore(super.getScore() + addedScore);
	}
	
	public double getAverage(){
		return super.getScore()/numOfReviews;
	}
}
