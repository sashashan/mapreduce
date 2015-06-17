package de.lmu.ifi.dbs.elki.distance.distancefunction;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialDistanceFunction;
import de.lmu.ifi.dbs.elki.utilities.HyperBoundingBox;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;

/**
 * Provides the Euclidean distance for FeatureVectors.
 * 
 * @author Arthur Zimek
 * @param <V> the type of FeatureVector to compute the distances in between
 */
public class EuclideanDistanceFunction<V extends NumberVector<V, ?>> extends AbstractDistanceFunction<V, DoubleDistance> implements SpatialDistanceFunction<V, DoubleDistance>, Parameterizable {
  /**
   * Provides a Euclidean distance function that can compute the Euclidean
   * distance (that is a DoubleDistance) for FeatureVectors.
   */
  public EuclideanDistanceFunction() {
    super(new DoubleDistance());
  }

  /**
   * Provides the Euclidean distance between the given two vectors.
   * 
   * @return the Euclidean distance between the given two vectors as an instance
   *         of {@link DoubleDistance DoubleDistance}.
   */
  @Override
  public DoubleDistance distance(V v1, V v2) {
    if(v1.getDimensionality() != v2.getDimensionality()) {
      throw new IllegalArgumentException("Different dimensionality of FeatureVectors" + "\n  first argument: " + v1.toString() + "\n  second argument: " + v2.toString() + "\n" + v1.getDimensionality() + "!=" + v2.getDimensionality());
    }
    double sqrDist = 0;
    for(int i = 1; i <= v1.getDimensionality(); i++) {
      double manhattanI = v1.doubleValue(i) - v2.doubleValue(i);
      sqrDist += manhattanI * manhattanI;
    }
    return new DoubleDistance(Math.sqrt(sqrDist));
  }

  @Override
  public DoubleDistance minDist(HyperBoundingBox mbr, V v) {
    if(mbr.getDimensionality() != v.getDimensionality()) {
      throw new IllegalArgumentException("Different dimensionality of objects\n  " + "first argument: " + mbr.toString() + "\n  " + "second argument: " + v.toString() + "\n" + mbr.getDimensionality() + "!=" + v.getDimensionality());
    }

    double sqrDist = 0;
    for(int d = 1; d <= v.getDimensionality(); d++) {
      double value = v.doubleValue(d);
      double r;
      if(value < mbr.getMin(d)) {
        r = mbr.getMin(d);
      }
      else if(value > mbr.getMax(d)) {
        r = mbr.getMax(d);
      }
      else {
        r = value;
      }

      double manhattanI = value - r;
      sqrDist += manhattanI * manhattanI;
    }
    return new DoubleDistance(Math.sqrt(sqrDist));
  }

  public DoubleDistance minDist(HyperBoundingBox mbr, Integer id) {
    return minDist(mbr, getDatabase().get(id));
  }

  public DoubleDistance distance(HyperBoundingBox mbr1, HyperBoundingBox mbr2) {
    if(mbr1.getDimensionality() != mbr2.getDimensionality()) {
      throw new IllegalArgumentException("Different dimensionality of objects\n  " + "first argument: " + mbr1.toString() + "\n  " + "second argument: " + mbr2.toString());
    }

    double sqrDist = 0;
    for(int d = 1; d <= mbr1.getDimensionality(); d++) {
      double m1, m2;
      if(mbr1.getMax(d) < mbr2.getMin(d)) {
        m1 = mbr1.getMax(d);
        m2 = mbr2.getMin(d);
      }
      else if(mbr1.getMin(d) > mbr2.getMax(d)) {
        m1 = mbr1.getMin(d);
        m2 = mbr2.getMax(d);
      }
      else { // The mbrs intersect!
        m1 = 0;
        m2 = 0;
      }
      double manhattanI = m1 - m2;
      sqrDist += manhattanI * manhattanI;
    }
    return new DoubleDistance(Math.sqrt(sqrDist));
  }

  public DoubleDistance centerDistance(HyperBoundingBox mbr1, HyperBoundingBox mbr2) {
    if(mbr1.getDimensionality() != mbr2.getDimensionality()) {
      throw new IllegalArgumentException("Different dimensionality of objects\n  " + "first argument: " + mbr1.toString() + "\n  " + "second argument: " + mbr2.toString());
    }

    double sqrDist = 0;
    for(int d = 1; d <= mbr1.getDimensionality(); d++) {
      double c1 = (mbr1.getMin(d) + mbr1.getMax(d)) / 2;
      double c2 = (mbr2.getMin(d) + mbr2.getMax(d)) / 2;

      double manhattanI = c1 - c2;
      sqrDist += manhattanI * manhattanI;
    }
    return new DoubleDistance(Math.sqrt(sqrDist));
  }
}
