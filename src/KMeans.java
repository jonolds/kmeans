import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class KMeans {
	private static final int K = 10, DIM = 20;
	private static final double THR = 0.01;
	private static ArrayList<List<Double>> cents = new ArrayList<>();

	static void kmeans(SparkSession ss) throws Exception {
		/* Read data to be processed and Initial Centroids*/
	    JavaRDD<List<Double>> points = ss.read().textFile("data.txt").javaRDD().map(d -> getPoints(d.split("\t")));
	    cents = getInitCentroid(ss);

	    int count = 0;
	    ArrayList<List<Double>> lastCents;
	    do {
	    	lastCents = copyCent(cents);
	    	JavaPairRDD<Integer, List<Double>> assigned = points.mapToPair(x->nearestC(x));
	    	JavaPairRDD<Integer, Tuple2<List<Double>, Integer>> assigned2 = assigned.mapToPair(x-> new Tuple2<>(x._1, new Tuple2<>(x._2,  1)));
	    	JavaPairRDD<Integer, Tuple2<List<Double>, Integer>> sums = assigned2.reduceByKey((v1, v2)-> new Tuple2<>(sumPoints(v1._1, v2._1), v1._2 + v2._2));
	    	JavaPairRDD<Integer, List<Double>> newCent = sums.mapToPair(x-> new Tuple2<>(x._1, divideByCount(x._2._1, x._2._2)));

	    	newCent.foreach(x->cents.set(x._1, x._2));
	    	count++;
	    }
	    while (diffCents(cents, lastCents) > THR);
	    System.out.println(count);
	    print(cents);
	}

	static List<Double> sumPoints(List<Double> p1, List<Double> p2) {
		return IntStream.range(0, DIM).mapToDouble(i->(p1.get(i) + p2.get(i))).boxed().collect(Collectors.toList());
	}
	static List<Double> divideByCount(List<Double> l, Integer cnt) {
		return IntStream.range(0, l.size()).mapToDouble(i->(l.get(i)/cnt)).boxed().collect(Collectors.toList());
	}

/* Updates Centroid - to be used after adding new points to a cluster */
	static Tuple2<Integer, List<Double>> updateCent(Tuple2<Integer, Iterable<List<Double>>> c) {
		for(List<Double> list : c._2)
			System.out.println(c._1 + ": " + list.get(0));
		// c.1 is the ID of the centroid. c.2 is the list of all the points assigned to the centroid.
		// TODO Computpe the average of all assigned points to update the centroid.
		return null;
	}

/* Finds the centroid closest to a point and returns a tuple of the centroid index and the point */
	static Tuple2<Integer, List<Double>> nearestC(List<Double> p) {
		double shortest_dist = Double.MAX_VALUE;
		int closest_cent = -1;
		for(int i = 0; i < cents.size(); i++) {
			double dist = distance(cents.get(i), p);
			if(dist < shortest_dist) {
				shortest_dist = dist;
				closest_cent = i;
			}
		}
		return new Tuple2<>(closest_cent, p);
	}

/*DISTANCE between two points*/
	static double distance(List<Double> p, List<Double> q) {
		return Math.sqrt(IntStream.range(0, DIM).mapToDouble(i->Math.pow(p.get(i)-q.get(i), 2)).sum());
	}
/* COMPARE CENTROIDS: Returns Double representing the difference between two sets of Centroids*/
	static double diffCents(ArrayList<List<Double>> c1, ArrayList<List<Double>> c2) {
		return  IntStream.range(0, K).mapToDouble(x->(distance(c1.get(x), c2.get(x)))).sum();
	}

/* PRINTING */
	static final int CENT_SPACING = 6;
	static void print(ArrayList<List<Double>> c) {
		for(List<?> l : c) {
			System.out.print("[ ");
			for(int i = 0; i < l.size(); i++)
				System.out.print(String.format("%-" + CENT_SPACING + "s" , l.get(i)));
			System.out.println("]");
		}
		System.out.println();
	}

/* Copy centroid (DEEP COPY) */
	static <T>ArrayList<List<T>> copyCent(ArrayList<List<T>> cent) {
		return (ArrayList<List<T>>)cent.stream().map(x->(x.stream().collect(Collectors.toList()))).collect(Collectors.toList());
	}
/* Returns initial Centroid from file*/
	static ArrayList<List<Double>> getInitCentroid(SparkSession ss) {
		return new ArrayList<>(ss.read().textFile("centroid.txt").javaRDD().map(x->getPoints(x.split("\t"))).collect());
	}
/* Convert String elments to Lists of Doubles*/
	private static List<Double> getPoints(String[] s_point) {
		return IntStream.range(0, DIM).mapToDouble(x->Double.parseDouble(s_point[x])).boxed().collect(Collectors.toList());
	}


/* Main / Standard Setup */
	public static void main(String[] args) throws Exception {
		SparkSession ss = settings();
		kmeans(ss);
//		Thread.sleep(20000);
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