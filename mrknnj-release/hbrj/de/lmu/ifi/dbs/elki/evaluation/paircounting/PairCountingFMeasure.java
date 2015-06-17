package de.lmu.ifi.dbs.elki.evaluation.paircounting;

import java.util.Collection;

import de.lmu.ifi.dbs.elki.data.Clustering;
import de.lmu.ifi.dbs.elki.data.cluster.Cluster;
import de.lmu.ifi.dbs.elki.data.model.Model;
import de.lmu.ifi.dbs.elki.evaluation.paircounting.generator.PairGeneratorMerge;
import de.lmu.ifi.dbs.elki.evaluation.paircounting.generator.PairGeneratorNoise;
import de.lmu.ifi.dbs.elki.evaluation.paircounting.generator.PairGeneratorSingleCluster;
import de.lmu.ifi.dbs.elki.evaluation.paircounting.generator.PairSortedGeneratorInterface;

/**
 * Compare two clustering results using a pair-counting F-Measure.
 * 
 * A pair are any two objects that belong to the same cluster.
 * 
 * Two clusterings are compared by comparing their pairs; if two clusterings completely agree,
 * they also agree on every pair; even when the clusters and points are ordered differently.
 * 
 * An empty clustering will of course have no pairs, the trivial all-in-one clustering
 * of course has n^2 pairs. Therefore neither recall nor precision itself are useful, however their
 * combination -- the F-Measure -- is useful.
 * 
 * @author Erich Schubert
 */
public class PairCountingFMeasure {
  /**
   * Get a pair generator for the given Clustering 
   * 
   * @param <R> Clustering result class
   * @param <M> Model type
   * @param clusters Clustering result
   * @return Sorted pair generator
   */
  public static <R extends Clustering<M>, M extends Model> PairSortedGeneratorInterface getPairGenerator(R clusters) {
    // collect all clusters into a flat list.
    Collection<Cluster<M>> allclusters = clusters.getAllClusters();

    // Make generators for each cluster
    PairSortedGeneratorInterface[] gens = new PairSortedGeneratorInterface[allclusters.size()];
    int i = 0;
    for (Cluster<?> c : allclusters) {
      if (c.isNoise()) {
        gens[i] = new PairGeneratorNoise(c);
      } else {
        gens[i] = new PairGeneratorSingleCluster(c);
      }
      i++;
    }
    // TODO: noise?
    return new PairGeneratorMerge(gens);
  }

  /**
   * Compare two clustering results.
   * 
   * @param <R> Result type
   * @param <M> Model type
   * @param <S> Result type
   * @param <N> Model type
   * @param result1 first result
   * @param result2 second result
   * @param beta Beta value for the F-Measure
   * @return Pair counting F-Measure result.
   */
  public static <R extends Clustering<M>, M extends Model, S extends Clustering<N>, N extends Model> double compareClusterings(R result1, S result2, double beta) {
    PairSortedGeneratorInterface first = getPairGenerator(result1);
    PairSortedGeneratorInterface second = getPairGenerator(result2);
    return countPairs(first, second, beta);
  }

  /**
   * Compare two clustering results.
   * 
   * @param <R> Result type
   * @param <M> Model type
   * @param <S> Result type
   * @param <N> Model type
   * @param result1 first result
   * @param result2 second result
   * @return Pair counting F-1-Measure result.
   */
  public static <R extends Clustering<M>, M extends Model, S extends Clustering<N>, N extends Model> double compareClusterings(R result1, S result2) {
    return compareClusterings(result1, result2, 1.0);
  }

  /**
   * Compare two sets of generated pairs.
   * 
   * @param first first set
   * @param second second set
   * @param beta beta value for F-Measure
   * @return F-beta-Measure for pairs.
   */
  private static double countPairs(PairSortedGeneratorInterface first, PairSortedGeneratorInterface second, double beta) {
    int inboth = 0;
    int infirst = 0;
    int insecond = 0;
    
    while (first.current() != null && second.current() != null) {
      int cmp = first.current().compareTo(second.current());
      if (cmp == 0) {
        inboth++;
        first.next();
        second.next();
      } else if (cmp < 0) {
        infirst++;
        first.next();
      } else {
        insecond++;
        second.next();
      }
    }
    while (first.current() != null) {
      infirst++;
      first.next();
    }
    while (second.current() != null) {
      insecond++;
      second.next();
    }
    
    //System.out.println("Both: "+inboth+" First: "+infirst+" Second: "+insecond);

    double fmeasure = ((1+beta*beta) * inboth) / ((1+beta*beta) * inboth + (beta*beta)*infirst + insecond);

    return fmeasure;
  }
}
