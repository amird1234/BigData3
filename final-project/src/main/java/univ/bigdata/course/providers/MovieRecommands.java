package univ.bigdata.course.providers;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;

import comparators.RecommandtionComperator;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import scala.Tuple2;
import scala.Tuple3;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.movie.User;

import static java.lang.Math.toIntExact;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieRecommands {
	private JavaRDD<MovieReview> MovsReviws;
	

    MatrixFactorizationModel model;
	JavaSparkContext sc;
	JavaPairRDD<String, Integer> moviesIndexed;
	JavaPairRDD<String, Integer> usersIndexed;
	
	
	public MovieRecommands(String traningFile){
		
		//creating the Spark context
        SparkConf conf = new SparkConf().setAppName("mySparkApp").setMaster("local");
        sc = new JavaSparkContext(conf);
        JavaRDD<String> fileLines = sc.textFile(traningFile);
        MovsReviws = fileLines.map(MovieReview::new);
        
        
        // creating distinct Movies RDD and indexing them to avoid creating two different indexes to same movie.
        // (ProductID, productIndex)
        moviesIndexed = MovsReviws.map(rev->rev.getMovie().getProductId()).distinct().zipWithIndex()
                										.mapToPair(pid->new Tuple2<>(pid._1, toIntExact(pid._2)));
//        Collection c = moviesIndexed.collect();
        // creating distinct Users RDD and indexing them to avoid creating two different indexes to same user.
        // (UserID, userIndex)
        usersIndexed = MovsReviws.map(rev->rev.getUserId()).distinct().zipWithIndex()
                										.mapToPair(uid->new Tuple2<>(uid._1, toIntExact(uid._2)));
//        usersIndexed.collect();
        JavaRDD<Rating> rating = 
        		//(pid,(uid,pid,score))
        		MovsReviws.mapToPair(review -> new Tuple2<>(review.getMovie().getProductId(),new Tuple3<>(review.getUserId(), review.getMovie().getProductId(), review.getMovie().getScore())))
        		//(pid,((uid,pid,score),productIndex))
        		.join(moviesIndexed)
        		// (uid,(productIndex, uid, score))
        		.mapToPair(review -> new Tuple2<>(review._2._1._1(), new Tuple3<>(review._2._2, review._2._1._1(), review._2._1._3())))
        		// (uid,(productIndex, uid, score), userIndex) 
        		.join(usersIndexed)
        		// Rating(userIndex,productIndex,score)
        		.map(temp -> new Rating(temp._2._2, temp._2._1._1(), temp._2._1._3()));
       
        model = ALS.train(JavaRDD.toRDD(rating), 10, 10, 0.01);
	}	

	public ArrayList<User> recommend(String usersFileName){
		ArrayList<User> UserRecs = null;
		try {
			List<String> lines = FileUtils.readLines(new File(usersFileName), "utf-8");
			lines.remove(0); lines.remove(0);
		    List<Tuple2<String, Integer>> predicts = usersIndexed
		                .filter(s -> lines.contains(s._1))
		                .collect();
		    UserRecs = new ArrayList<>();
		    if(!predicts.isEmpty()){
			    for(Tuple2<String, Integer> currentuser : predicts){
			    	//(MovieReview)
			    	JavaPairRDD<Double, String> currentUserPredicts = model.predict(MovsReviws.filter(review -> !review.getUserId().equals(currentuser._1))
			    			//(pid,score)
			    			.mapToPair(review -> new Tuple2<>(review.getMovie().getProductId(), null))
			    			//(pidS,null)
			    			.distinct()
			    			//(pidS(null,pid))
			    			.join(moviesIndexed)
			    			//(uid,pid)
			    			.mapToPair(recommand -> new Tuple2<>(currentuser._2, recommand._2._2)))
			    	//(rating,product)
			    	.mapToPair(predict -> new Tuple2<>(predict.product(), predict.rating()))
			    	//(pid(rating,pidS))
			    	.join(moviesIndexed.mapToPair(movie -> new Tuple2<>(movie._2, movie._1)))
			    	//(
			    	.mapToPair(f -> new Tuple2<>(f._2._1, f._2._2));
			    	List<Tuple2<Double, String>> recommendations;
			    	recommendations = currentUserPredicts.takeOrdered(10,new RecommandtionComperator());

			    	UserRecs.add(new User(currentuser._1,recommendations));
			    }
		    }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return UserRecs;
		
	}
	
	
}
