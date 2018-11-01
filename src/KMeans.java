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
import java.util.function.Function;

@SuppressWarnings("unused")
public class KMeans {

	private static final int K = 10, DIM = 20;
	private static final double THR = 0.01;
	private static ArrayList<List<Double>> centroid = new ArrayList<>();

	static void kmeans(SparkSession ss) throws Exception {

	/* Read data to be processed */
	    JavaRDD<String> data1 = ss.read().textFile("data.txt").toJavaRDD();
	    JavaRDD<List<Double>> points = data1.map(d -> getPoints(d));

	/* Read initial centroids */
	    List<String> data2 = ss.read().textFile("centroid.txt").collectAsList();
	    for(int i=0; i<K; i++)
			centroid.add(getPoints(data2.get(i)));

	    ArrayList<List<Double>> updateCent = new ArrayList<>(centroid);

	    do {
	    	centroid = new ArrayList<List<Double>>(updateCent);
	    	// TODO Assign points
	    	// TODO Update centroids

	    } while (diff(centroid, updateCent)>THR);

	    for (int i=0; i<K; i++)
	    	System.out.println(centroid.get(i));
	}

	private static double diff(ArrayList<List<Double>> c1, ArrayList<List<Double>> c2) {
		// TODO Compute the sum of distances for each pair of centroids, one from c1 and the other from c2
		return 0;
	}

	private static Tuple2<Integer, List<Double>> update(Tuple2<Integer, Iterable<List<Double>>> c) {
		// c.1 is the ID of the centroid. c.2 is the list of all the points assigned to the centroid.
		// TODO Computpe the average of all assigned points to update the centroid.
		return null;
	}

	private static Tuple2<Integer, List<Double>> nearestC(List<Double> p) {
		// p is one point
		// TODO Find the nearest centroid to p, and produce the tuple (centroidID, p)
		return null;
	}

/*DISTANCE between two points*/
	private static double distance(List<Double> p, List<Double> q) {
		double distance = 0.0;
		for (int i=0; i<DIM; i++)
			distance += (p.get(i)-q.get(i))*(p.get(i)-q.get(i));
		return Math.sqrt(distance);
	}

/* Convert String elments to Lists of Doubles*/
	private static List<Double> getPoints(String d) {
		String[] s_point = d.split("\t");
		Double[] d_point = new Double[DIM];
		for (int i=0; i<DIM; i++)
			d_point[i] = Double.parseDouble(s_point[i]);
		return Arrays.asList(d_point);
	}
}

/* TEST ArrayList<List<Double>> Deep Copy !!!WRONG */
//for(List l : centroid)
//updateCent.add(new ArrayList<Double>(l));
//printCentroids(centroid);
//for(int i = 0; i < 3; i++)
//for(int k = 0; k < 20; k++)
//	updateCent.get(i).set(k, 9.9);
//printCentroids(updateCent);
//printCentroids(centroid);