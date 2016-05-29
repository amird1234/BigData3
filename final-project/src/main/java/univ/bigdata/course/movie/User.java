/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class User {
	String userID;
	Integer numerator, denominator;
	Double helpfulness;
	Integer numOfReviews;
	Boolean helpFull = false;

	public User(String userID, Integer numerator, Integer denominator, Integer numOfReviews) {
		super();
		this.userID = userID;
		
		this.numerator = numerator;
		this.denominator = denominator;
		this.numOfReviews = numOfReviews;

		this.calcHelpfulness();
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
}
