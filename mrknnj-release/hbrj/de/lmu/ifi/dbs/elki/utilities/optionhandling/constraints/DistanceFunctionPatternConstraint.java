package de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints;

import de.lmu.ifi.dbs.elki.distance.distancefunction.DistanceFunction;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.ParameterException;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.WrongParameterValueException;

/**
 * Parameter constraint class for testing if a given pattern parameter (
 * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.StringParameter}
 * ) holds a valid pattern for a specific distance function (
 * {@link de.lmu.ifi.dbs.elki.distance.distancefunction.DistanceFunction}).
 * 
 * @author Steffi Wanka
 */
@Deprecated
public class DistanceFunctionPatternConstraint implements ParameterConstraint<String> {
  /**
   * The distance function the pattern is checked for.
   */
  private DistanceFunction<?, ?> distanceFunction;

  /**
   * Constructs a distance function pattern constraint for testing if a given
   * pattern parameter holds a valid pattern for the parameter {@code
   * distFunction}
   * 
   * @param distFunction the distance function the pattern is checked for
   */
  public DistanceFunctionPatternConstraint(DistanceFunction<?, ?> distFunction) {
    this.distanceFunction = distFunction;
  }

  /**
   * Checks if the given pattern parameter holds a valid pattern for the
   * distance function. If not so, a parameter exception (
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.ParameterException}) is
   * thrown.
   * 
   */
  public void test(String t) throws ParameterException {
    try {
      distanceFunction.valueOf(t);
    }
    catch(IllegalArgumentException ex) {
      throw new WrongParameterValueException("The specified pattern " + t + " is not valid " + "for distance function " + distanceFunction.getClass().getName() + ".\n" + ex.getMessage());
    }
  }

  public String getDescription(String parameterName) {
    return parameterName + " must be suitable to " + distanceFunction;
  }
}