/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
package univ.bigdata.course.movie;

import java.util.Date;

import scala.Serializable;

public class MovieReview implements Serializable{

	public static final String proIDPrefix = "product/productId:";
	public static final String userIDPrefix = "review/userId:";
	public static final String profNamePrefix = "review/profileName:";
	public static final String helpPrefix = "review/helpfulness:";
	public static final String scorePrefix = "review/score:";
	public static final String timePrefix = "review/time:";
	public static final String summeryPrefix = "review/summary:";
	public static final String textPrefix = "review/text:";

    private Movie movie;

    private String userId;

    private String profileName;

    private String helpfulness;

    private Date timestamp;

    private String summary;

    private String review;

    public MovieReview() {
    }

    public MovieReview(String line) {
		String productId;
		Double score;

		// parse current line (movie review)
		productId = line.substring((line.indexOf(proIDPrefix) + proIDPrefix.length() + 1),
				line.indexOf(userIDPrefix) - 1);
		score = Double.valueOf(
				line.substring((line.indexOf(scorePrefix) + scorePrefix.length() + 1), (line.indexOf(timePrefix) - 1)));
		movie = new Movie(productId, score);

		// create movie review
		userId = line.substring((line.indexOf(userIDPrefix) + userIDPrefix.length() + 1),
				line.indexOf(profNamePrefix) - 1);
		profileName = line.substring((line.indexOf(profNamePrefix) + profNamePrefix.length() + 1),
				line.indexOf(helpPrefix) - 1);
		helpfulness = line.substring((line.indexOf(helpPrefix) + helpPrefix.length() + 1),
				line.indexOf(scorePrefix) - 1);
		summary = line.substring((line.indexOf(summeryPrefix) + summeryPrefix.length() + 1),
				line.indexOf(textPrefix) - 1);
		review = line.substring(line.indexOf(textPrefix) + textPrefix.length() + 1);
		timestamp = new Date(Long.valueOf(
				line.substring((line.indexOf(timePrefix) + timePrefix.length() + 1), line.indexOf(summeryPrefix) - 1)));
    }
    
    public MovieReview(Movie movie, String userId, String profileName, String helpfulness, Date timestamp, String summary, String review) {
        this.movie = movie;
        this.userId = userId;
        this.profileName = profileName;
        this.helpfulness = helpfulness;
        this.timestamp = timestamp;
        this.summary = summary;
        this.review = review;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }

    public String getHelpfulness() {
        return helpfulness;
    }

    public void setHelpfulness(String helpfulness) {
        this.helpfulness = helpfulness;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public Movie getMovie() {
        return movie;
    }

    public void setMovie(Movie movie) {
        this.movie = movie;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    @Override
    public String toString() {
        return "MovieReview{" +
                "movie=" + movie +
                ", userId='" + userId + '\'' +
                ", profileName='" + profileName + '\'' +
                ", helpfulness='" + helpfulness + '\'' +
                ", timestamp=" + timestamp +
                ", summary='" + summary + '\'' +
                ", review='" + review + '\'' +
                '}';
    }
}