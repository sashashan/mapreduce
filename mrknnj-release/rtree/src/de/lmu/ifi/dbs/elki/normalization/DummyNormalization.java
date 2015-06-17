package de.lmu.ifi.dbs.elki.normalization;

import java.util.List;

import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.database.Associations;
import de.lmu.ifi.dbs.elki.logging.AbstractLoggable;
import de.lmu.ifi.dbs.elki.math.linearalgebra.LinearEquationSystem;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;
import de.lmu.ifi.dbs.elki.utilities.pairs.Pair;

/**
 * Dummy normalization that does nothing. This class is used at normalization of
 * multi-represented objects if one representation needs no normalization.
 * 
 * @author Elke Achtert
 * @param <O> object type
 */
@Title("Dummy normalization that does nothing")
@Description("This class is used at normalization of multi-represented objects if one representation needs no normalization.")
public class DummyNormalization<O extends DatabaseObject> extends AbstractLoggable implements Normalization<O>, Parameterizable {
  /**
   * @return the specified objectAndAssociationsList
   */
  public List<Pair<O, Associations>> normalizeObjects(List<Pair<O, Associations>> objectAndAssociationsList) {
    return objectAndAssociationsList;
  }

  /**
   * @return the specified featureVectors
   */
  public List<O> normalize(List<O> featureVectors) {
    return featureVectors;
  }

  /**
   * @return the specified featureVectors
   */
  public List<O> restore(List<O> featureVectors) {
    return featureVectors;
  }

  /**
   * @return the specified featureVector
   */
  public O restore(O featureVector) {
    return featureVector;
  }

  /**
   * @return the specified linear equation system
   */
  public LinearEquationSystem transform(LinearEquationSystem linearEquationSystem) {
    return linearEquationSystem;
  }

  public String toString(String pre) {
    return pre + toString();
  }

  /**
   * Returns a string representation of this object.
   * 
   * @return a string representation of this object
   */
  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
