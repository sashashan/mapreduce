package de.lmu.ifi.dbs.elki.distance.similarityfunction;

import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.distance.Distance;
import de.lmu.ifi.dbs.elki.distance.MeasurementFunction;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;

/**
 * Interface SimilarityFunction describes the requirements of any similarity
 * function.
 *
 * @author Elke Achtert 
 * @param <O> object type
 * @param <D> distance type
 */
public interface SimilarityFunction<O extends DatabaseObject, D extends Distance<D>> extends MeasurementFunction<O, D>, Parameterizable {
  /**
   * Returns the similarity between the two objects specified by their object ids.
   *
   * @param id1 first object id
   * @param id2 second object id
   * @return the similarity between the two objects specified by their object ids
   */
  D similarity(Integer id1, Integer id2);

  /**
   * Returns the similarity between the two specified objects.
   *
   * @param id1 first object id
   * @param o2  second DatabaseObject
   * @return the similarity between the two objects specified by their object ids
   */
  D similarity(Integer id1, O o2);

  /**
   * Computes the similarity between two given DatabaseObjects according to this
   * similarity function.
   *
   * @param o1 first DatabaseObject
   * @param o2 second DatabaseObject
   * @return the similarity between two given DatabaseObjects according to this
   *         similarity function
   */
  D similarity(O o1, O o2);
}