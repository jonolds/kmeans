import scala.Tuple2;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("unused")
public class Kmeans {

	private static int K = 10, DIM = 20;
	private static double THR = 0.01;
	private static ArrayList<List<Double>> centroid;


	static void kmeans() throws Exception {
		SparkSession ss = settings();
	    JavaRDD<String> data1 = ss.read().textFile("data.txt").toJavaRDD();
	    JavaRDD<List<Double>> points = data1.map(d -> getPoints(d));

	    // Read the centroids
	    List<String> data2 = ss.read().textFile("centroid.txt").collectAsList();
	    for (int i=0; i<K; i++) {
			centroid.add(getPoints(data2.get(i)));
	    }

	    ArrayList<List<Double>> updateCentroid = new ArrayList<List<Double>>(centroid);

	    do {
	    	centroid = new ArrayList<List<Double>>(updateCentroid);
	    	// TODO Assign points
	    	// TODO Update centroids

	    } while (diff(centroid, updateCentroid)>THR);


	    for (int i=0; i<K; i++) {
	    	System.out.println(centroid.get(i));
	    }

	}

	private static double diff(ArrayList<List<Double>> c1, ArrayList<List<Double>> c2) {
		// TODO Compute the sum of distances for each pair of centroids, one from c1 and the other from c2
		return 0;
	}

	private static Tuple2<Integer, List<Double>> update(Tuple2<Integer, Iterable<List<Double>>> c) {
		// c.1 is the ID of the centroid. c.2 is the list of all the points assigned to the centroid.
		// TODO Compute the average of all assigned points to update the centroid.
		return null;
	}

	private static Tuple2<Integer, List<Double>> nearestC(List<Double> p) {
		// p is one point
		// TODO Find the nearest centroid to p, and produce the tuple (centroidID, p)
		return null;
	}

	private static double dist(List<Double> p, List<Double> q) {
		// Compute the Euclidean distance between two points p and q
		double distance = 0.0;

		for (int i=0; i<DIM; i++) {
			distance += (p.get(i)-q.get(i))*(p.get(i)-q.get(i));
		}
		return Math.sqrt(distance);
	}

	private static List<Double> getPoints(String d) {
		String[] s_point = d.split("\t");
		Double[] d_point = new Double[DIM];
		for (int i=0; i<DIM; i++) {
			d_point[i] = Double.parseDouble(s_point[i]);
		}
		return Arrays.asList(d_point);
	}


	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("Kmeans").config("spark.master", "local").config("spark.eventlog.enabled","true").config("spark.executor.cores", "2").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
	public static void main(String[] args) throws Exception {
		kmeans();
		Thread.sleep(20000);
	}
}