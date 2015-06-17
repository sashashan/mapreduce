package de.lmu.ifi.dbs.elki.algorithm.clustering;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.lmu.ifi.dbs.elki.algorithm.AbstractAlgorithm;
import de.lmu.ifi.dbs.elki.data.Clustering;
import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.data.DatabaseObjectGroup;
import de.lmu.ifi.dbs.elki.data.DatabaseObjectGroupCollection;
import de.lmu.ifi.dbs.elki.data.cluster.Cluster;
import de.lmu.ifi.dbs.elki.data.model.ClusterModel;
import de.lmu.ifi.dbs.elki.data.model.Model;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.distance.Distance;
import de.lmu.ifi.dbs.elki.distance.IntegerDistance;
import de.lmu.ifi.dbs.elki.distance.similarityfunction.SharedNearestNeighborSimilarityFunction;
import de.lmu.ifi.dbs.elki.logging.progress.FiniteProgress;
import de.lmu.ifi.dbs.elki.logging.progress.IndefiniteProgress;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Reference;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.GreaterConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;

/**
 * <p>
 * Shared nearest neighbor clustering.
 * </p>
 * <p>
 * Reference: L. Ertöz, M. Steinbach, V. Kumar: Finding Clusters of Different
 * Sizes, Shapes, and Densities in Noisy, High Dimensional Data. <br>
 * In: Proc. of SIAM Data Mining (SDM), 2003.
 * </p>
 * 
 * @author Arthur Zimek
 * @param <O> the type of DatabaseObject the algorithm is applied on
 * @param <D> the type of Distance used for the preprocessing of the shared
 *        nearest neighbors neighborhood lists
 */
@Title("SNN: Shared Nearest Neighbor Clustering")
@Description("Algorithm to find shared-nearest-neighbors-density-connected sets in a database based on the " + "parameters 'minPts' and 'epsilon' (specifying a volume). " + "These two parameters determine a density threshold for clustering.")
@Reference(authors = "L. Ertöz, M. Steinbach, V. Kumar", title = "Finding Clusters of Different Sizes, Shapes, and Densities in Noisy, High Dimensional Data", booktitle = "Proc. of SIAM Data Mining (SDM), 2003", url="http://www.siam.org/meetings/sdm03/proceedings/sdm03_05.pdf")
public class SNNClustering<O extends DatabaseObject, D extends Distance<D>> extends AbstractAlgorithm<O, Clustering<Model>> implements ClusteringAlgorithm<Clustering<Model>, O> {
  /**
   * OptionID for {@link #EPSILON_PARAM}
   */
  public static final OptionID EPSILON_ID = OptionID.getOrCreateOptionID("snn.epsilon", "The minimum SNN density.");

  /**
   * Parameter to specify the minimum SNN density, must be an integer greater
   * than 0.
   * <p>
   * Key: {@code -snn.epsilon}
   * </p>
   */
  private final IntParameter EPSILON_PARAM = new IntParameter(EPSILON_ID, new GreaterConstraint(0));

  /**
   * Holds the value of {@link #EPSILON_PARAM}.
   */
  private IntegerDistance epsilon;

  /**
   * OptionID for {@link #MINPTS_PARAM}
   */
  public static final OptionID MINPTS_ID = OptionID.getOrCreateOptionID("snn.minpts", "Threshold for minimum number of points in " + "the epsilon-SNN-neighborhood of a point.");

  /**
   * Parameter to specify the threshold for minimum number of points in the
   * epsilon-SNN-neighborhood of a point, must be an integer greater than 0.
   * <p>
   * Key: {@code -snn.minpts}
   * </p>
   */
  private final IntParameter MINPTS_PARAM = new IntParameter(MINPTS_ID, new GreaterConstraint(0));

  /**
   * Holds the value of {@link #MINPTS_PARAM}.
   */
  private int minpts;

  /**
   * Holds a list of clusters found.
   */
  protected List<List<Integer>> resultList;

  /**
   * Holds a set of noise.
   */
  protected Set<Integer> noise;

  /**
   * Holds a set of processed ids.
   */
  protected Set<Integer> processedIDs;

  /**
   * The similarity function for the shared nearest neighbor similarity.
   */
  private SharedNearestNeighborSimilarityFunction<O, D> similarityFunction;

  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public SNNClustering(Parameterization config) {
    super(config);
    if(config.grab(EPSILON_PARAM)) {
      epsilon = new IntegerDistance(EPSILON_PARAM.getValue());
    }
    if(config.grab(MINPTS_PARAM)) {
      minpts = MINPTS_PARAM.getValue();
    }

    similarityFunction = new SharedNearestNeighborSimilarityFunction<O, D>(config);
  }

  /**
   * Performs the SNN clustering algorithm on the given database.
   */
  @Override
  protected Clustering<Model> runInTime(Database<O> database) {
    FiniteProgress objprog = new FiniteProgress("Clustering", database.size());
    IndefiniteProgress clusprog = new IndefiniteProgress("Number of clusters");
    resultList = new ArrayList<List<Integer>>();
    noise = new HashSet<Integer>();
    processedIDs = new HashSet<Integer>(database.size());
    similarityFunction.setDatabase(database);
    if(logger.isVerbose()) {
      logger.verbose("Clustering:");
    }
    if(database.size() >= minpts) {
      for(Integer id : database) {
        if(!processedIDs.contains(id)) {
          expandCluster(database, id, objprog, clusprog);
          if(processedIDs.size() == database.size() && noise.size() == 0) {
            break;
          }
        }
        if(logger.isVerbose()) {
          objprog.setProcessed(processedIDs.size());
          clusprog.setProcessed(resultList.size());
          logger.progress(objprog);
          logger.progress(clusprog);
        }
      }
    }
    else {
      for(Integer id : database) {
        noise.add(id);
        if(logger.isVerbose()) {
          objprog.setProcessed(noise.size());
          clusprog.setProcessed(resultList.size());
          logger.progress(objprog);
          logger.progress(clusprog);
        }
      }
    }
    // signal completion.
    clusprog.setCompleted();
    logger.progress(clusprog);

    Clustering<Model> result = new Clustering<Model>();
    for(Iterator<List<Integer>> resultListIter = resultList.iterator(); resultListIter.hasNext();) {
      DatabaseObjectGroup group = new DatabaseObjectGroupCollection<List<Integer>>(resultListIter.next());
      result.addCluster(new Cluster<Model>(group, ClusterModel.CLUSTER));
    }
    DatabaseObjectGroup group = new DatabaseObjectGroupCollection<Set<Integer>>(noise);
    result.addCluster(new Cluster<Model>(group, true, ClusterModel.CLUSTER));

    return result;
  }

  /**
   * Returns the shared nearest neighbors of the specified query object in the
   * given database.
   * 
   * @param database the database holding the objects
   * @param queryObject the query object
   * @return the shared nearest neighbors of the specified query object in the
   *         given database
   */
  protected List<Integer> findSNNNeighbors(Database<O> database, Integer queryObject) {
    List<Integer> neighbors = new LinkedList<Integer>();
    for(Integer id : database) {
      if(similarityFunction.similarity(queryObject, id).compareTo(epsilon) >= 0) {
        neighbors.add(id);
      }
    }
    return neighbors;
  }

  /**
   * DBSCAN-function expandCluster adapted to SNN criterion.
   * <p/>
   * <p/>
   * Border-Objects become members of the first possible cluster.
   * 
   * @param database the database on which the algorithm is run
   * @param startObjectID potential seed of a new potential cluster
   * @param objprog the progress object to report about the progress of
   *        clustering
   */
  protected void expandCluster(Database<O> database, Integer startObjectID, FiniteProgress objprog, IndefiniteProgress clusprog) {
    List<Integer> seeds = findSNNNeighbors(database, startObjectID);

    // startObject is no core-object
    if(seeds.size() < minpts) {
      noise.add(startObjectID);
      processedIDs.add(startObjectID);
      if(logger.isVerbose()) {
        objprog.setProcessed(processedIDs.size());
        clusprog.setProcessed(resultList.size());
        logger.progress(objprog);
        logger.progress(clusprog);
      }
      return;
    }

    // try to expand the cluster
    List<Integer> currentCluster = new ArrayList<Integer>();
    for(Integer seed : seeds) {
      if(!processedIDs.contains(seed)) {
        currentCluster.add(seed);
        processedIDs.add(seed);
      }
      else if(noise.contains(seed)) {
        currentCluster.add(seed);
        noise.remove(seed);
      }
    }
    seeds.remove(0);

    while(seeds.size() > 0) {
      Integer o = seeds.remove(0);
      List<Integer> neighborhood = findSNNNeighbors(database, o);

      if(neighborhood.size() >= minpts) {
        for(Integer p : neighborhood) {
          boolean inNoise = noise.contains(p);
          boolean unclassified = !processedIDs.contains(p);
          if(inNoise || unclassified) {
            if(unclassified) {
              seeds.add(p);
            }
            currentCluster.add(p);
            processedIDs.add(p);
            if(inNoise) {
              noise.remove(p);
            }
          }
        }
      }

      if(logger.isVerbose()) {
        objprog.setProcessed(processedIDs.size());
        int numClusters = currentCluster.size() > minpts ? resultList.size() + 1 : resultList.size();
        clusprog.setProcessed(numClusters);
        logger.progress(objprog);
        logger.progress(clusprog);
      }

      if(processedIDs.size() == database.size() && noise.size() == 0) {
        break;
      }
    }
    if(currentCluster.size() >= minpts) {
      resultList.add(currentCluster);
    }
    else {
      for(Integer id : currentCluster) {
        noise.add(id);
      }
      noise.add(startObjectID);
      processedIDs.add(startObjectID);
    }
  }

  /**
   * Returns the value of {@link #EPSILON_PARAM}.
   * 
   * @return the value of {@link #EPSILON_PARAM}
   */
  public IntegerDistance getEpsilon() {
    return epsilon;
  }
}