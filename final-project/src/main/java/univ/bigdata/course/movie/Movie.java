/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

import java.util.Comparator;

import scala.Serializable;

public class Movie implements Serializable, Comparable<Movie>{

    private String productId;

    private double score;

    public Movie() {
    }

    public Movie(String productId, double score) {
        this.productId = productId;
        this.score = score;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "productId='" + productId + '\'' +
                ", score=" + score +
                '}';
    }
    @Override
    public int compareTo(Movie other){
        double diff = this.getScore()- other.getScore();
        int res;
        if (diff > 0){
        	res = -1;
        }else {
        	if (diff < 0){
        		res = 1;
        	}else{
        		res = 0;
        	}
        }
        return res == 0 ? this.getProductId().compareTo(other.getProductId()) : res;
    	
    }
    public static Comparator<Movie> FruitNameComparator 
    = new Comparator<Movie>() {

	public int compare(Movie movie1, Movie movie2) {
		double comp = movie1.getScore() - movie2.getScore();
		if(comp == 0){
			return movie1.getProductId().compareTo(movie2.getProductId());
		}else {
			if (comp >0){
				return 1;
			}else {
				return -1;
			}
		}
	}

};

}
