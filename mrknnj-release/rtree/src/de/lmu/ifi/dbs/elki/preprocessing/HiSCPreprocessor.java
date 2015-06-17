package de.lmu.ifi.dbs.elki.preprocessing;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import de.lmu.ifi.dbs.elki.algorithm.clustering.subspace.HiSC;
import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.database.AssociationID;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.database.DistanceResultPair;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.distance.distancefunction.DistanceFunction;
import de.lmu.ifi.dbs.elki.distance.distancefunction.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.logging.AbstractLoggable;
import de.lmu.ifi.dbs.elki.logging.progress.FiniteProgress;
import de.lmu.ifi.dbs.elki.utilities.DatabaseUtil;
import de.lmu.ifi.dbs.elki.utilities.ExceptionMessages;
import de.lmu.ifi.dbs.elki.utilities.FormatUtil;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.GreaterConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.IntervalConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.DoubleParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;

/**
 * Preprocessor for HiSC preference vector assignment to objects of a certain
 * database.
 * 
 * @author Elke Achtert
 * @param <V> the type of NumberVector handled by the preprocessor
 * @see HiSC
 */
@Title("HiSC Preprocessor")
@Description("Computes the preference vector of objects of a certain database according to the HiSC algorithm.")
public class HiSCPreprocessor<V extends NumberVector<V, ?>> extends AbstractLoggable implements PreferenceVectorPreprocessor<V>, Parameterizable {
  /**
   * The default value for alpha.
   */
  public static final double DEFAULT_ALPHA = 0.01;

  /**
   * OptionID for {@link #ALPHA_PARAM}
   */
  public static final OptionID ALPHA_ID = OptionID.getOrCreateOptionID("hisc.alpha", "The maximum absolute variance along a coordinate axis.");

  /**
   * The maximum absolute variance along a coordinate axis. Must be in the range
   * of [0.0, 1.0).
   * <p>
   * Default value: {@link #DEFAULT_ALPHA}
   * </p>
   * <p>
   * Key: {@code -hisc.alpha}
   * </p>
   */
  private final DoubleParameter ALPHA_PARAM = new DoubleParameter(ALPHA_ID, new IntervalConstraint(0.0, IntervalConstraint.IntervalBoundary.OPEN, 1.0, IntervalConstraint.IntervalBoundary.OPEN), DEFAULT_ALPHA);

  /**
   * Holds the value of parameter {@link #ALPHA_PARAM}.
   */
  private double alpha;

  /**
   * OptionID for {@link #K_PARAM}.
   */
  public static final OptionID K_ID = OptionID.getOrCreateOptionID("hisc.k", "The number of nearest neighbors considered to determine the preference vector. If this value is not defined, k ist set to three times of the dimensionality of the database objects.");

  /**
   * The number of nearest neighbors considered to determine the preference
   * vector. If this value is not defined, k is set to three times of the
   * dimensionality of the database objects.
   * <p>
   * Key: {@code -hisc.k}
   * </p>
   * <p>
   * Default value: three times of the dimensionality of the database objects
   * </p>
   */
  private final IntParameter K_PARAM = new IntParameter(K_ID, new GreaterConstraint(0), true);

  /**
   * Holds the value of parameter {@link #K_PARAM}.
   */
  private Integer k;

  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public HiSCPreprocessor(Parameterization config) {
    super();

    // parameter alpha
    if(config.grab(ALPHA_PARAM)) {
      alpha = ALPHA_PARAM.getValue();
    }

    // parameter k
    if(config.grab(K_PARAM)) {
      k = K_PARAM.getValue();
    }
  }

  public void run(Database<V> database, @SuppressWarnings("unused") boolean verbose, boolean time) {
    if(database == null || database.size() <= 0) {
      throw new IllegalArgumentException(ExceptionMessages.DATABASE_EMPTY);
    }

    StringBuffer msg = new StringBuffer();

    long start = System.currentTimeMillis();
    FiniteProgress progress = new FiniteProgress("Preprocessing preference vector", database.size());

    if(k == null) {
      V obj = database.get(database.iterator().next());
      k = 3 * obj.getDimensionality();
    }

    DistanceFunction<V, DoubleDistance> distanceFunction = new EuclideanDistanceFunction<V>();
    distanceFunction.setDatabase(database);

    Iterator<Integer> it = database.iterator();
    int processed = 1;
    while(it.hasNext()) {
      Integer id = it.next();

      if(logger.isDebugging()) {
        msg.append("\n\nid = ").append(id);
        msg.append(" ").append(database.getAssociation(AssociationID.LABEL, id));
        msg.append("\n knns: ");
      }

      List<DistanceResultPair<DoubleDistance>> knns = database.kNNQueryForID(id, k, distanceFunction);
      List<Integer> knnIDs = new ArrayList<Integer>(knns.size());
      for(DistanceResultPair<DoubleDistance> knn : knns) {
        knnIDs.add(knn.getID());
        if(logger.isDebugging()) {
          msg.append(database.getAssociation(AssociationID.LABEL, knn.getID())).append(" ");
        }
      }

      BitSet preferenceVector = determinePreferenceVector(database, id, knnIDs, msg);
      database.associate(AssociationID.PREFERENCE_VECTOR, id, preferenceVector);
      progress.setProcessed(processed++);

      if(logger.isVerbose()) {
        logger.progress(progress);
      }
    }

    if(logger.isDebugging()) {
      logger.debugFine(msg.toString());
    }

    long end = System.currentTimeMillis();
    if(time) {
      long elapsedTime = end - start;
      logger.verbose(this.getClass().getName() + " runtime: " + elapsedTime + " milliseconds.");
    }
  }

  /**
   * Returns the value of the alpha parameter (i.e. the maximum allowed variance
   * along a coordinate axis).
   * 
   * @return the value of the alpha parameter
   */
  public double getAlpha() {
    return alpha;
  }

  /**
   * Returns the value of the k parameter (i.e. the number of nearest neighbors
   * considered to determine the preference vector).
   * 
   * @return the value of the k parameter
   */
  public int getK() {
    return k;
  }

  /**
   * Determines the preference vector according to the specified neighbor ids.
   * 
   * @param database the database storing the objects
   * @param id the id of the object for which the preference vector should be
   *        determined
   * @param neighborIDs the ids of the neighbors
   * @param msg a string buffer for debug messages
   * @return the preference vector
   */
  private BitSet determinePreferenceVector(Database<V> database, Integer id, List<Integer> neighborIDs, StringBuffer msg) {
    // variances
    double[] variances = DatabaseUtil.variances(database, database.get(id), neighborIDs);

    // preference vector
    BitSet preferenceVector = new BitSet(variances.length);
    for(int d = 0; d < variances.length; d++) {
      if(variances[d] < alpha) {
        preferenceVector.set(d);
      }
    }

    if(msg != null && logger.isDebugging()) {
      msg.append("\nalpha " + alpha);
      msg.append("\nvariances ");
      msg.append(FormatUtil.format(variances, ", ", 4));
      msg.append("\npreference ");
      msg.append(FormatUtil.format(variances.length, preferenceVector));
    }

    return preferenceVector;
  }
}