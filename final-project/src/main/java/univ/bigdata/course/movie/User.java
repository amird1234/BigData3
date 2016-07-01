/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.mllib.recommendation.Rating;

import comparators.RecommandtionComperator;
import scala.Serializable;
import scala.Tuple2;

public class User implements Serializable,Comparable<User>{
	String userID;
	Integer numerator, denominator;
	Double helpfulness;
	Integer numOfReviews;
	Boolean helpFull = false;
	List<Tuple2<Double, String>> recommendations;

	public User(String userID, Integer numerator, Integer denominator, Integer numOfReviews) {
		super();
		this.userID = userID;
		
		this.numerator = numerator;
		this.denominator = denominator;
		this.numOfReviews = numOfReviews;

		this.calcHelpfulness();
	}
	
	public User(String userID, List<Tuple2<Double, String>> Recommendations){
		this.userID = userID;
		this.recommendations = new ArrayList<>(Recommendations);
//				Recommendations;
		this.recommendations.sort(new RecommandtionComperator());
	}

	public User(String string, Rating[] _2) {
		userID = string;
	}
	public User(String userID, Double helpfulness){
		this.userID = userID;
		this.helpfulness = helpfulness;
	}
	private Double round(Double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	
	private void calcHelpfulness(){
		if (this.denominator != 0) {
			this.helpfulness = (double)((double)this.numerator/(double)this.denominator);
			this.helpFull = true;
		} else {
			this.helpfulness = 0.0;
		}
		
	}
	
	public Boolean isHelpfull() {
		return this.helpFull;
	}

	public Integer getNumOfReviews() {
		return numOfReviews;
	}

	public void incNumOfReviews() {
		this.numOfReviews++;
	}
	
	public Double getHelpfullness() {
		return this.round(this.helpfulness, 5);
	}

	public String getUserID() {
		return userID;
	}

	public void incNumeratorDeniminator(Integer numeratorDelta, Integer denominatorDelta) {
		this.numerator += numeratorDelta;
		this.denominator += denominatorDelta;
		this.calcHelpfulness();
	}
	
	public String printRecommendations(){
		Integer i= 1;
		String recString = "Recommendations for " + userID + ":\n";
		for(Tuple2<Double, String> recommand : recommendations){
			recString += i + ". " + recommand._2 + " Score:" + recommand._1 + "\n";
			i++;
		}
		recString += "======================================";
		
		return recString;
	}
    @Override
    public int compareTo(User other){
        double diff = this.getHelpfullness()- other.getHelpfullness();
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
        return res == 0 ? this.getUserID().compareTo(other.getUserID()) : res;
    	
    }
}
