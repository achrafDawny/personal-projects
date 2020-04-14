import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import tools.Application;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SparkWordCount7 {
    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\Users\\aelgdaou\\Downloads\\achraf\\cours\\spark\\hadoop");
        Application app = new Application();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = javaSparkContext.textFile(app.getFileFromResource("data.csv"));

        JavaPairRDD<String, Integer> flattenPairs = inputRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
                String[] textArray = s.split(" ");
                for(String word: textArray){
                    tupleList.add(new Tuple2<String, Integer>(word, 1));
                }
                return tupleList.iterator();
            }
        });

        JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCountRDD.saveAsTextFile("spark-java/src/main/resources/result1");
    }


}
