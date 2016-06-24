package univ.bigdata.course;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;
import univ.bigdata.course.movie.PageRankResults;

import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterables;

import comparators.PageRankComperator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 */
public final class JavaPageRank {
  private static final Pattern SPACES = Pattern.compile("\\s+");

//  static void showWarning() {
//    String warning = "WARN: This is a naive implementation of PageRank " +
//            "and is given as an example! \n" +
//            "Please use the PageRank implementation found in " +
//            "org.apache.spark.graphx.lib.PageRank for more conventional use.";
//    System.err.println(warning);
//  }

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static List<PageRankResults> Rank(JavaRDD<String> ranksrdd, int numOfIterations) throws Exception {


//    showWarning();
//
//    SparkSession spark = SparkSession
//      .builder()
//      .appName("JavaPageRank")
//      .getOrCreate();

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    
    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    //change main to function that gets JavaRDD and number of iterations. 
    /////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////
    JavaRDD<String> lines = ranksrdd;
    
    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = lines.
    		mapToPair( (PairFunction<String, String, String>) s->{
          String[] parts = SPACES.split(s);
          return new Tuple2<>(parts[0], parts[1]);
      }).distinct().groupByKey().cache();

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues( (Function<Iterable<String>, Double>) rs -> 1.0);

    // Calculates and updates URL ranks continuously using PageRank algorithm.
    for (int current = 0; current < numOfIterations; current++) {
      // Calculates URL contributions to the rank of other URLs.
      JavaPairRDD<String, Double> contribs = links.join(ranks).values()
        .flatMapToPair((PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>) s ->{
            int urlCount = Iterables.size(s._1);
            List<Tuple2<String, Double>> results = new ArrayList<>();
            for (String n : s._1) {
              results.add(new Tuple2<>(n, s._2() / urlCount));
            }
            return results;
      });

      // Re-calculates URL ranks based on neighbor contributions.
      ranks = contribs.reduceByKey(new Sum()).mapValues((Function<Double, Double>) sum -> 0.15 + sum * 0.85);
//      List<Tuple2<String, Double>> t2 = contribs.collect();
//      t2.clear();
    }
//    List<Tuple2<String, Double>> t = ranks.collect();
    // Collects all URL ranks and dump them to console.
    JavaRDD<PageRankResults> output = ranks.map(o -> new PageRankResults(o._1, o._2));
//    List<PageRankResults> t = output.collect();
//    List<PageRankResults> t1 = output.collect();
    return output.collect();//.sort(new PageRankComperator());

//    spark.stop();
  }
}
