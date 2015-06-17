package de.lmu.ifi.dbs.elki.algorithm;

import java.util.Iterator;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.distance.distancefunction.DistanceFunction;
import de.lmu.ifi.dbs.elki.distance.distancefunction.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.result.Result;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;

/**
 * Dummy Algorithm, which just iterates over all points once, doing a 10NN query
 * each. Useful in testing e.g. index structures and as template for custom
 * algorithms.
 * 
 * @author Erich Schubert
 * @param <V> Vector type
 */
@Title("Dummy Algorithm")
@Description("The algorithm executes a 10NN query on all data points, and can be used in unit testing")
public class DummyAlgorithm<V extends NumberVector<V, ?>> extends AbstractAlgorithm<V, Result> {
  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public DummyAlgorithm(Parameterization config) {
    super(config);
  }

  /**
   * Iterates over all points in the database.
   */
  @Override
  protected Result runInTime(Database<V> database) throws IllegalStateException {
    DistanceFunction<V, DoubleDistance> distFunc = new EuclideanDistanceFunction<V>();
    for(Iterator<Integer> iter = database.iterator(); iter.hasNext();) {
      Integer id = iter.next();
      database.get(id);
      // run a 10NN query for each point.
      database.kNNQueryForID(id, 10, distFunc);
    }
    return null;
  }
}
