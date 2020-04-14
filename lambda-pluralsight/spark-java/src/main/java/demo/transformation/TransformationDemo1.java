package demo.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.App;
import scala.Tuple2;
import tools.Application;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationDemo1 {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\aelgdaou\\Downloads\\achraf\\cours\\spark\\hadoop");
        Application app = new Application();
        SparkConf conf = new SparkConf()
                .setAppName("SparkForJava")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("OFF");
        List<Integer> intList = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> intRDD = sc.parallelize(intList, 2);

                        // map
        // Java 7
        intRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 + 1;
            }
        });

        // Java 8
        intRDD.map(v1 -> v1 + 1);
                        // filter
        // Java 7
        intRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        // Java 8
        intRDD.filter(v1 -> v1 % 2 ==0);
                        // flatMap
        JavaRDD<String> stringRDD = sc.parallelize(Arrays.asList("Hello Spark", "Hello Java"));
        // Java 7
        stringRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        // Java 8
        stringRDD.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
                        // mapToPair
        // Java 7
        JavaPairRDD<String, Integer> pairRDD7 = intRDD.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer i) throws Exception {
                if(i % 2 == 0)
                    return new Tuple2<>("even", i);
                else
                    return new Tuple2<>("odd", i);
            }
        });

        // Java 8
        JavaPairRDD<String, Integer> pairRDD8 = intRDD.mapToPair(i -> (i % 2 == 0 ?new Tuple2<String, Integer>("even", i) :
                new Tuple2<String, Integer>("odd", i)));
                        // flatMapToPair
        // Java 7
        stringRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String,Integer>> list =new ArrayList<>();
                for (String token : s.split(" ")) {
                    list.add(new Tuple2<String, Integer>(token, token.length()));
                }
                return list.iterator();
            }
        });

        // Java 8
        stringRDD.flatMapToPair(s -> Arrays.asList(s.split(" ")).stream()
                .map(s1 -> new Tuple2<String, Integer>(s1, s1.length())).iterator());
                        // union
        JavaRDD<Integer> intRDD2 = sc.parallelize(Arrays.asList(1,2,3));
        intRDD.union(intRDD2);
                        // intersection
        intRDD.intersection(intRDD2);
                        // distinct
        JavaRDD<Integer> rddWithDup = sc.parallelize(Arrays.asList(1,1,2,4,5,6,8,8,9,10,11,11));
        rddWithDup.distinct();
                        // cartesian
        JavaRDD<String> rddString = sc.parallelize(Arrays.asList("A","B","C"));
        JavaRDD<Integer> rddInteger = sc.parallelize(Arrays.asList(1,4,5));
        rddString.cartesian(rddInteger);
                        // groupByKey
        JavaPairRDD<String, Iterable<Integer>> groupedRDD = pairRDD8.groupByKey();
                        // reduceByKey
        // Java 7
        JavaPairRDD<String, Integer> reducedRDD7 = pairRDD7.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // Java 8
        JavaPairRDD<String, Integer> reducedRDD = pairRDD8.reduceByKey((v1, v2) -> v1+v2);
                        // sortByKey
        JavaPairRDD<String, Integer> unsortedRDD = sc.parallelizePairs(Arrays.asList(new Tuple2<>("B",2), new Tuple2<>("C",5)
        ,new Tuple2<>("D", 7), new Tuple2<>("A", 8)));
        JavaPairRDD<String, Integer> sortedRDD = unsortedRDD.sortByKey();
                        // Join
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinedRDD = sortedRDD.join(unsortedRDD);
                        // CoGroup
        JavaPairRDD<String, String> pairRDD1 = sc.parallelizePairs(Arrays.asList(new Tuple2<String, String>("B", "A"),
                new Tuple2<String, String>("B", "D"), new Tuple2<String, String>("A", "E"), new Tuple2<String, String>("A", "B")));

        JavaPairRDD<String, Integer> pairRDD2 = sc.parallelizePairs(Arrays.asList(new Tuple2<String, Integer>("B", 2),
                new Tuple2<String, Integer>("B", 5), new Tuple2<String, Integer>("A", 7), new Tuple2<String, Integer>("A", 8)));

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> coGroupedRDD = pairRDD1.cogroup(pairRDD2);
        coGroupedRDD.foreach(s -> System.out.println(s));

    }
}
