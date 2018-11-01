import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

@SuppressWarnings("unused")
public class KMeans {

	private static final int K = 10, DIM = 20;
	private static final double THR = 0.01;
	private static ArrayList<List<Double>> centroid = new ArrayList<>();

	static void kmeans(SparkSession ss) throws Exception {
		/* Read data to be processed */
	    JavaRDD<List<Double>> points = ss.read().textFile("data.txt").javaRDD().map(d -> getPoints(d));
	    /* Read initial centroids */
	    centroid = getInitCentroid(ss);

	    testCopyCent(centroid);


//	    printCent(centroid);
//	    ArrayList<List<Double>> updatedCent = copyCent(centroid);
//	    do {
//	    	centroid = new ArrayList<List<Double>>(updatedCent);
//	    	// TODO Assign points
//	    	// TODO Update centroids
//
//	    } while (diffCents(centroid, updatedCent) > THR);

	}

/* Returns Double representing the difference between two sets of Centroids*/
	private static double diffCents(ArrayList<List<Double>> c1, ArrayList<List<Double>> c2) {
		// TODO Compute the sum of distances for each pair of centroids, one from c1 and the other from c2
		return 0;
	}

/* Updates Centroid - to be used after adding new points to a cluster */
	private static Tuple2<Integer, List<Double>> updateCent(Tuple2<Integer, Iterable<List<Double>>> c) {
		// c.1 is the ID of the centroid. c.2 is the list of all the points assigned to the centroid.
		// TODO Computpe the average of all assigned points to update the centroid.
		return null;
	}

/* Finds the centroid closest to a point and returns a tuple of the centroid index and the point */
	private static Tuple2<Integer, List<Double>> nearestC(List<Double> p) {
		double shortest_dist = -1;
		int closest_cent = -1;

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



/* Copy centroid (DEEP COPY) */
	static ArrayList<List<Double>> copyCent(ArrayList<List<Double>> cent) {


		ArrayList<List<Double>> copy = new ArrayList<>();
		for(int r = 0; r < K; r++) {
			List<Double> list = new ArrayList<>();
			for(int c = 0; c < DIM; c++)
				list.add(cent.get(r).get(c));
			copy.add(list);
		}

		ArrayList<List<Double>> copy2 = new ArrayList<>();
		for(int r = 0; r < K; r++) {
			List<Double> list = cent.get(r).stream().collect(Collectors.toList());
			copy2.add(list);
		}



//		ArrayList<List<Double>> copy2 = cent.stream().map(SomeObject::getId).collect(Collectors.toList());
		return copy2;
	}

/* Returns initial Centroid from file*/
	static ArrayList<List<Double>> getInitCentroid(SparkSession ss) {
		ArrayList<List<Double>> initCent = new ArrayList<>();
		List<String> data2 = ss.read().textFile("centroid.txt").collectAsList();
		for (int i=0; i<K; i++)
			initCent.add(getPoints(data2.get(i)));
		return initCent;
	}

/* Convert String elments to Lists of Doubles*/
	private static List<Double> getPoints(String d) {
		String[] s_point = d.split("\t");
		Double[] d_point = new Double[DIM];
		for (int i=0; i<DIM; i++)
			d_point[i] = Double.parseDouble(s_point[i]);
		return Arrays.asList(d_point);
	}


/* PRINTING */
	static final int CENT_SPACING = 6;
	static void printCent(ArrayList<List<Double>> c) {
//		printCentHeader();
		for(List<?> l : c) {
			System.out.print("[ ");
			for(int i = 0; i < l.size(); i++)
				System.out.print(String.format("%-" + CENT_SPACING + "s" , l.get(i)));
			System.out.println("]");
		}
		System.out.println();
	}
	static void printCentHeader() {
		System.out.print("[ ");
		for(int i = 0; i < DIM; i++)
			System.out.print(String.format("%-" + CENT_SPACING + "s" , i));
		System.out.println("]");

		System.out.print("  ");
		for(int i = 0; i < DIM; i++)
			System.out.print(String.format("%-" + CENT_SPACING + "s" , "____"));
		System.out.println("]");
	}


/* TESTING */

	static void testCopyCent(ArrayList<List<Double>> cOrig) {
	    //Print origical centroid
	    printCent(centroid);
	    //Copy centroid
	    ArrayList<List<Double>> copy = copyCent(centroid);
	    //Change values in copy of centroid
	    List<Double> badLine = new ArrayList<Double>(Collections.nCopies(20, 8.8));
	    copy.replaceAll(x->badLine);
	    //Print copy of centroid to demonstrate changes
	    printCent(copy);
	    //prient original centroid to verify it remains the same
	    printCent(centroid);
	}



/* Main / Standard Setup */
	public static void main(String[] args) throws Exception {
		SparkSession ss = settings();
		kmeans(ss);
		Thread.sleep(20000);
		ss.close();
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
}

/* Testing used to verify copyCent deep copy */
////Print origical centroid
//printCent(centroid);
////Copy centroid
//ArrayList<List<Double>> copy = copyCent(centroid);
////Change values in copy of centroid
//List<Double> badLine = new ArrayList<Double>(Collections.nCopies(20, 8.8));
//copy.replaceAll(x->badLine);
////Print copy of centroid to demonstrate changes
//printCent(copy);
////prient original centroid to verify it remains the same
//printCent(centroid);