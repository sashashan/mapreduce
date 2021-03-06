package de.lmu.ifi.dbs.elki.distance.distancefunction.adapter;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;

/**
 * Adapter from a normalized similarity function to a distance function using <code>1 - sim</code>.
 * 
 * @author Erich Schubert
 *
 * @param <V> Vector class to process.
 */
public class SimilarityAdapterLinear<V extends NumberVector<V,?>> extends SimilarityAdapterAbstract<V> {
  /**
   * Constructor.
   * 
   * @param config Configuration
   */
  public SimilarityAdapterLinear(Parameterization config) {
    super(config);
  }

  /**
   * Distance implementation
   */
  @Override
  public DoubleDistance distance(V v1, V v2) {
    DoubleDistance sim = similarityFunction.similarity(v1, v2);
    return new DoubleDistance(1.0 - sim.doubleValue());
  }
}
