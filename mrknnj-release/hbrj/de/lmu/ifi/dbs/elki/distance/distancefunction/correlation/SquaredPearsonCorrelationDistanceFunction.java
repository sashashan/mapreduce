package de.lmu.ifi.dbs.elki.distance.distancefunction.correlation;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.distance.distancefunction.AbstractDistanceFunction;
import de.lmu.ifi.dbs.elki.math.MathUtil;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;

/**
 * Squared Pearson correlation distance function for feature vectors.
 * 
 * The squared Pearson correlation distance is computed from the Pearson correlation coefficient <code>r</code> as: <code>1-r</code><sup><code>2</code></sup>.
 * Hence, possible values of this distance are between 0 and 1.
 * 
 * The distance between two vectors will be low (near 0), if their attribute values are dimension-wise strictly positively or negatively correlated.
 * For Features with uncorrelated attributes, the distance value will be high (near 1).
 * 
 * @author Arthur Zimek
 * @param <V> the type of FeatureVector to compute the distances in between
 * @param <N> the type of Number of the attributes of vectors of type V
 */
public class SquaredPearsonCorrelationDistanceFunction<V extends NumberVector<V,N>, N extends Number> extends AbstractDistanceFunction<V, DoubleDistance> implements Parameterizable {
  /**
   * Provides a SquaredPearsonCorrelationDistanceFunction.
   */
  public SquaredPearsonCorrelationDistanceFunction() {
    super(new DoubleDistance());
  }

  /**
   * Computes the squared Pearson correlation distance for two given feature vectors.
   * 
   * The squared Pearson correlation distance is computed from the Pearson correlation coefficient <code>r</code> as: <code>1-r</code><sup><code>2</code></sup>.
   * Hence, possible values of this distance are between 0 and 1.
   *
   * @param v1 first feature vector
   * @param v2 second feature vector
   * @return the squared Pearson correlation distance for two given feature vectors v1 and v2
   */
  public DoubleDistance distance(V v1, V v2) {
    double pcc = MathUtil.pearsonCorrelationCoefficient(v1, v2);
    return new DoubleDistance(1 - pcc * pcc);
  }
}