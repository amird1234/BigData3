package univ.bigdata.course.movie;


import scala.Serializable;

/**
 * pageRankResults class from page rank.
 */
public class PageRankResults implements  Serializable {
    /**
	 * 
	 */
	public String id;
    public Double score;

    public PageRankResults(String id, Double score) {
        this.id = id;
        this.score = score;
    }

    @Override
    public String toString() {
        return "pageRankResults{" +
                "UserId='" + id + '\'' +
                ", PageRank=" + score +
                '}';
    }

//    @Override
//    public int compareTo(PageRankResults pageRankResults) {
//    	try{
//        if (this.score == pageRankResults.score){
//            return this.id.compareTo(pageRankResults.id) * -1;
//        }
//        else {
//            return this.score > pageRankResults.score ? 1 : -1;
//        }
//    	}catch(Exception e){
//    		return 0;
//    	}
//    }

}