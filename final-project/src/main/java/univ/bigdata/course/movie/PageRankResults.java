package univ.bigdata.course.movie;


import java.math.BigDecimal;

import org.apache.commons.math3.util.Precision;
import org.apache.spark.sql.catalyst.expressions.Round;

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
                ", PageRank=" + Precision.round(score, 5, BigDecimal.ROUND_HALF_UP) +
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