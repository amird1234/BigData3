package univ.bigdata.course.providers;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.Rating;

import com.esotericsoftware.minlog.Log;

import comparators.RecommandtionComperator;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.movie.User;
import univ.bigdata.course.movie.UserHashed;

import static java.lang.Math.toIntExact;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MovieRecommands {
	private JavaRDD<MovieReview> MovsReviws;
	

    MatrixFactorizationModel model;
	JavaSparkContext sc;
	JavaPairRDD<String, Integer> moviesIndexed;
	JavaPairRDD<String, Integer> usersIndexed;
	Double MAPSum = 0.0;
	Double MAPCounter = 0.0;
	Double MAPValue;
	
	//constructor for movie recommendations
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
	
	//Constructor for Movie MAP
	/**
	 *algorithm (the way i see it)
	 *
	 * 1) create a (user,movie) tuples from predict (i assume movies in rdd arrived sorted...)
	 * 2) give an index TO tuple - i saw recommendations online to use zipwithindex (gives an ordered index for all movies)
	 * 3) in the "map" of "2" we should map tuple to ((user, movie), index) -> No Need, done automatically in "2"
	 * 4) get all movies from test set (besides score<3.0)
	 * 5) join with map function that maps only if user & movie match.
	 * 6) calculate MAP: after join we should index result (again using some indexing method - maybe zipwithindex) 
	 * 7) now we have (user, movie, iredictIndex, testSetIndex) - relative index is 1,2,3,4... index is 1,3,5,,8,11 etc
	 * 8) after we have this 4-tuple we can calculate independently (just like we did in my NotePad++ and we don't care about 
	 * 	  doing it on 10000000 computers, it is just a map, sum and division 
	 * 
	 * in general i just documented, didn't do anything crucial 
	 * @param traningFile
	 * @param testFile
	 */
	public MovieRecommands(String traningFile, String testFile){
		// Build a model based on the training file
		this(traningFile);
		

		Integer counter = 0;
		
		JavaRDD<String> testRecString = sc.textFile(testFile);
		
//		List<Tuple2<String, Integer>> userString = usersIndexed.collect();
		JavaRDD<MovieReview> relevantMoviesAll = testRecString
				.map(MovieReview::new)
				.filter(MovieR -> MovieR.getMovie().getScore() >= 3);
		
        // (UserID, userIndex)
		JavaPairRDD<String, Integer> usersIndexedTest = relevantMoviesAll.map(rev->rev.getUserId()).distinct().zipWithIndex()
                										.mapToPair(uid->new Tuple2<>(uid._1, toIntExact(uid._2)));
        
		List<Tuple2<String, Integer>> userString = usersIndexed.join(usersIndexedTest).mapToPair(f -> new Tuple2<>(f._1, f._2._1)).collect();
		
        if(!userString.isEmpty()){
		    for(Tuple2<String, Integer> currentuser : userString){
		    	
		    	counter++;
		    	//1) create a (user,movie) tuples from predict sorted by rating
		    	//(MovieReview)
		    	JavaPairRDD<String, String> currentUserPredicts = model.predict(MovsReviws.filter(review -> !review.getUserId().equals(currentuser._1))
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
		    	//(pid, (rating,pidS))
		    	.join(moviesIndexed.mapToPair(movie -> new Tuple2<>(movie._2, movie._1)))
		    	//(
		    	.mapToPair(recommand -> new Tuple2<>(recommand._2._1, new Tuple2<>(currentuser._1, recommand._2._2)))
		    	.sortByKey(false)
		    	.mapToPair(f -> new Tuple2<>(f._2._1, f._2._2));
		    	
		    	//2) give an index TO tuple now we have ( (uid, pid), recommendIndexZip)
		    	JavaPairRDD<Tuple2<String, String>,Integer> currentUserPredictsIndexed = currentUserPredicts.zipWithIndex().mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._1,f._1._2),toIntExact(f._2)));
    			
				//4) get all movies from test set (besides score<3.0)
				// Take only those with score 3 or higher
		    	JavaPairRDD<Tuple2<String,String>, Integer> relevantMovies = relevantMoviesAll
						.mapToPair(f -> new Tuple2<>(new Tuple2<>(f.getUserId(),f.getMovie().getProductId()),null));
				
				//5) join with filter function that let records get in only if user & movie match.
				JavaPairRDD<Tuple2<String, String>, Tuple2<Integer, Integer>> finalRDD = 
				//((uid,pid),(recommendIndexZip,null))
				currentUserPredictsIndexed.join(relevantMovies)
				//6) calculate MAP: after join we should index result
				.zipWithIndex()
				//7) now we have (user, movie, iredictIndex, testSetIndex) - relative index is 1,2,3,4... index is 1,3,5,,8,11 etc
				//((uid,pid),(recommendIndexZip,testSetIndex))
				.mapToPair(f -> new Tuple2<>(new Tuple2<>(f._1._1._1,f._1._1._2),new Tuple2<>(f._1._2._1,toIntExact(f._2))));
		    
				//8) after we have this 4-tuple we can calculate independently.
				MAPSum += (finalRDD.mapToDouble(f -> (f._2._2 / f._2._1))).sum()/finalRDD.count();
				MAPCounter++;
		    }
		    MAPValue = MAPSum/MAPCounter;		
		}
        
	}
	
	public Double getMAPValue(){
		return MAPValue;
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
	
	
	private JavaRDD<MovieReview> getReleventUserMovies(Tuple2<String, Integer> user, JavaRDD<MovieReview> set){
		JavaPairRDD<String,MovieReview> generalSet = set.mapToPair(f -> new Tuple2<>(f.getMovie().getProductId(), f));
		JavaPairRDD<String,MovieReview> userSet = set.filter(f -> f.getUserId().equals(user._1)).mapToPair(f -> new Tuple2<>(f.getMovie().getProductId(), f));
		return generalSet.subtractByKey(userSet).map(f -> f._2);
	
	}
	
	
}
