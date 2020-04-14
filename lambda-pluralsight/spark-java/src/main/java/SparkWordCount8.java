import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import tools.Application;

import java.util.Arrays;

public class SparkWordCount8 {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\aelgdaou\\Downloads\\achraf\\cours\\spark\\hadoop");
        Application app = new Application();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
        JavaSparkContext javaSC = new JavaSparkContext(conf);
        JavaRDD<String> inputRDD = javaSC.textFile(app.getFileFromResource("data.csv"));
        JavaPairRDD<String, Integer> flattenPairs = inputRDD.flatMapToPair(line ->
            Arrays.asList(line.split(" ")).stream()
                    .map(word -> new Tuple2<String, Integer>(word, 1))
                    .iterator()
        );
        JavaPairRDD<String, Integer> wordCountRDD = flattenPairs.reduceByKey((v1, v2) -> v1 + v2);
        wordCountRDD.saveAsTextFile("spark-java/src/main/resources/result2");
    }
}
