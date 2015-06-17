package de.lmu.ifi.dbs.elki.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import de.lmu.ifi.dbs.elki.data.KNNList;
import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.database.AssociationID;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.database.DistanceResultPair;
import de.lmu.ifi.dbs.elki.database.SpatialIndexDatabase;
import de.lmu.ifi.dbs.elki.distance.Distance;
import de.lmu.ifi.dbs.elki.distance.DistanceUtil;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialDistanceFunction;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialEntry;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialNode;
import de.lmu.ifi.dbs.elki.logging.progress.FiniteProgress;
import de.lmu.ifi.dbs.elki.logging.progress.IndefiniteProgress;
import de.lmu.ifi.dbs.elki.result.AnnotationFromHashMap;
import de.lmu.ifi.dbs.elki.utilities.HyperBoundingBox;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.GreaterConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;

/**
 * Joins in a given spatial database to each object its k-nearest neighbors.
 * This algorithm only supports spatial databases based on a spatial index
 * structure.
 * 
 * @author Elke Achtert
 * @param <V> the type of FeatureVector handled by this Algorithm
 * @param <D> the type of Distance used by this Algorithm
 * @param <N> the type of node used in the spatial index structure
 * @param <E> the type of entry used in the spatial node
 */
@Title("K-Nearest Neighbor Join")
@Description("Algorithm to find the k-nearest neighbors of each object in a spatial database")
public class KNNJoin<V extends NumberVector<V, ?>, D extends Distance<D>, N extends SpatialNode<N, E>, E extends SpatialEntry> extends DistanceBasedAlgorithm<V, D, AnnotationFromHashMap<KNNList<D>>> {
  /**
   * OptionID for {@link #K_PARAM}
   */
  public static final OptionID K_ID = OptionID.getOrCreateOptionID("knnjoin.k", "Specifies the k-nearest neighbors to be assigned.");

  /**
   * Parameter that specifies the k-nearest neighbors to be assigned, must be an
   * integer greater than 0.
   * <p>
   * Default value: {@code 1}
   * </p>
   * <p>
   * Key: {@code -knnjoin.k}
   * </p>
   */
  public final IntParameter K_PARAM = new IntParameter(K_ID, new GreaterConstraint(0), 1);

  /**
   * Association ID for KNNLists.
   */
  public static final AssociationID<KNNList<?>> KNNLIST = AssociationID.getOrCreateAssociationIDGenerics("KNNS", KNNList.class);

  /**
   * The k parameter
   */
  int k;

  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public KNNJoin(Parameterization config) {
    super(config);
    if(config.grab(K_PARAM)) {
      k = K_PARAM.getValue();
    }
  }

  /**
   * Joins in the given spatial database to each object its k-nearest neighbors.
   * 
   * @throws IllegalStateException if the specified database is not an instance
   *         of {@link SpatialIndexDatabase} or the specified distance function
   *         is not an instance of {@link SpatialDistanceFunction}.
   */
  @Override
  @SuppressWarnings("unchecked")
  protected AnnotationFromHashMap<KNNList<D>> runInTime(Database<V> database) throws IllegalStateException {
    if(!(database instanceof SpatialIndexDatabase)) {
      throw new IllegalStateException("Database must be an instance of " + SpatialIndexDatabase.class.getName());
    }
    if(!(getDistanceFunction() instanceof SpatialDistanceFunction)) {
      throw new IllegalStateException("Distance Function must be an instance of " + SpatialDistanceFunction.class.getName());
    }
    SpatialIndexDatabase<V, N, E> db = (SpatialIndexDatabase<V, N, E>) database;
    SpatialDistanceFunction<V, D> distFunction = (SpatialDistanceFunction<V, D>) getDistanceFunction();
    distFunction.setDatabase(db);

    HashMap<Integer, KNNList<D>> knnLists = new HashMap<Integer, KNNList<D>>();

    try {
      // data pages of s
      List<E> ps_candidates = db.getLeaves();
      FiniteProgress progress = new FiniteProgress(this.getClass().getName(), db.size());
      IndefiniteProgress pageprog = new IndefiniteProgress("Number of processed data pages");
      if(logger.isDebugging()) {
        logger.debugFine("# ps = " + ps_candidates.size());
      }
      // data pages of r
      List<E> pr_candidates = new ArrayList<E>(ps_candidates);
      if(logger.isDebugging()) {
        logger.debugFine("# pr = " + pr_candidates.size());
      }
      int processed = 0;
      int processedPages = 0;
      boolean up = true;
      for(E pr_entry : pr_candidates) {
        HyperBoundingBox pr_mbr = pr_entry.getMBR();
        N pr = db.getIndex().getNode(pr_entry);
        D pr_knn_distance = distFunction.infiniteDistance();
        if(logger.isDebugging()) {
          logger.debugFine(" ------ PR = " + pr);
        }
        // create for each data object a knn list
        for(int j = 0; j < pr.getNumEntries(); j++) {
          knnLists.put(pr.getEntry(j).getID(), new KNNList<D>(k, getDistanceFunction().infiniteDistance()));
        }

        if(up) {
          for(E ps_entry : ps_candidates) {
            HyperBoundingBox ps_mbr = ps_entry.getMBR();
            D distance = distFunction.distance(pr_mbr, ps_mbr);

            if(distance.compareTo(pr_knn_distance) <= 0) {
              N ps = db.getIndex().getNode(ps_entry);
              pr_knn_distance = processDataPages(pr, ps, knnLists, pr_knn_distance);
            }
          }
          up = false;
        }

        else {
          for(int s = ps_candidates.size() - 1; s >= 0; s--) {
            E ps_entry = ps_candidates.get(s);
            HyperBoundingBox ps_mbr = ps_entry.getMBR();
            D distance = distFunction.distance(pr_mbr, ps_mbr);

            if(distance.compareTo(pr_knn_distance) <= 0) {
              N ps = db.getIndex().getNode(ps_entry);
              pr_knn_distance = processDataPages(pr, ps, knnLists, pr_knn_distance);
            }
          }
          up = true;
        }

        processed += pr.getNumEntries();

        if(logger.isVerbose()) {
          progress.setProcessed(processed);
          pageprog.setProcessed(processedPages++);
          logger.progress(progress);
          logger.progress(pageprog);
        }
      }
      pageprog.setCompleted();
      logger.progress(pageprog);
      return new AnnotationFromHashMap(KNNLIST, knnLists);
    }

    catch(Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Processes the two data pages pr and ps and determines the k-nearest
   * neighbors of pr in ps.
   * 
   * @param pr the first data page
   * @param ps the second data page
   * @param knnLists the knn lists for each data object
   * @param pr_knn_distance the current knn distance of data page pr
   * @return the k-nearest neighbor distance of pr in ps
   */
  private D processDataPages(N pr, N ps, HashMap<Integer, KNNList<D>> knnLists, D pr_knn_distance) {

    // noinspection unchecked
    boolean infinite = pr_knn_distance.isInfiniteDistance();
    for(int i = 0; i < pr.getNumEntries(); i++) {
      Integer r_id = pr.getEntry(i).getID();
      KNNList<D> knnList = knnLists.get(r_id);

      for(int j = 0; j < ps.getNumEntries(); j++) {
        Integer s_id = ps.getEntry(j).getID();

        D distance = getDistanceFunction().distance(r_id, s_id);
        if(knnList.add(new DistanceResultPair<D>(distance, s_id))) {
          // set kNN distance of r
          if(infinite) {
            pr_knn_distance = knnList.getMaximumDistance();
          }
          pr_knn_distance = DistanceUtil.max(knnList.getMaximumDistance(), pr_knn_distance);
        }
      }
    }
    return pr_knn_distance;
  }
}
