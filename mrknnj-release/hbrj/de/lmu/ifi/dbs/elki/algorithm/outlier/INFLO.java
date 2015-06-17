package de.lmu.ifi.dbs.elki.algorithm.outlier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import de.lmu.ifi.dbs.elki.algorithm.DistanceBasedAlgorithm;
import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.database.AssociationID;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.database.DistanceResultPair;
import de.lmu.ifi.dbs.elki.distance.DoubleDistance;
import de.lmu.ifi.dbs.elki.math.MinMax;
import de.lmu.ifi.dbs.elki.result.AnnotationFromHashMap;
import de.lmu.ifi.dbs.elki.result.AnnotationResult;
import de.lmu.ifi.dbs.elki.result.MultiResult;
import de.lmu.ifi.dbs.elki.result.OrderingFromHashMap;
import de.lmu.ifi.dbs.elki.result.OrderingResult;
import de.lmu.ifi.dbs.elki.result.outlier.OutlierResult;
import de.lmu.ifi.dbs.elki.result.outlier.OutlierScoreMeta;
import de.lmu.ifi.dbs.elki.result.outlier.QuotientOutlierScoreMeta;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Reference;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.GreaterConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.DoubleParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;

/**
 * INFLO provides the Mining Algorithms (Two-way Search Method) for Influence
 * Outliers using Symmetric Relationship
 * <p>
 * Reference: <br>
 * <p>
 * Jin, W., Tung, A., Han, J., and Wang, W. 2006<br />
 * Ranking outliers using symmetric neighborhood relationship< br/>
 * In Proc. Pacific-Asia Conf. on Knowledge Discovery and Data Mining (PAKDD),
 * Singapore
 * </p>
 * 
 * @author Ahmed Hettab
 * @param <O> the type of DatabaseObject the algorithm is applied on
 */
@Title("INFLO: Influenced Outlierness Factor")
@Description("Ranking Outliers Using Symmetric Neigborhood Relationship")
@Reference(authors = "Jin, W., Tung, A., Han, J., and Wang, W", title = "Ranking outliers using symmetric neighborhood relationship", booktitle = "Proc. Pacific-Asia Conf. on Knowledge Discovery and Data Mining (PAKDD), Singapore, 2006", url = "http://dx.doi.org/10.1007/11731139_68")
public class INFLO<O extends DatabaseObject> extends DistanceBasedAlgorithm<O, DoubleDistance, MultiResult> {
  /**
   * OptionID for {@link #M_PARAM}
   */
  public static final OptionID M_ID = OptionID.getOrCreateOptionID("inflo.m", "The threshold");

  /**
   * Parameter to specify if any object is a Core Object must be a double
   * greater than 0.0
   * <p>
   * see paper "Two-way search method" 3.2
   * <p>
   * Key: {@code -inflo.m}
   * </p>
   */
  private final DoubleParameter M_PARAM = new DoubleParameter(M_ID, new GreaterConstraint(0.0), 1.0);

  /**
   * Holds the value of {@link #M_PARAM}.
   */
  private double m;

  /**
   * OptionID for {@link #K_PARAM}
   */
  public static final OptionID K_ID = OptionID.getOrCreateOptionID("inflo.k", "The number of nearest neighbors of an object to be considered for computing its INFLO_SCORE.");

  /**
   * Parameter to specify the number of nearest neighbors of an object to be
   * considered for computing its INFLO_SCORE. must be an integer greater than
   * 1.
   * <p>
   * Key: {@code -inflo.k}
   * </p>
   */
  private final IntParameter K_PARAM = new IntParameter(K_ID, new GreaterConstraint(1));

  /**
   * Holds the value of {@link #K_PARAM}.
   */
  private int k;

  /**
   * The association id to associate the INFLO_SCORE of an object for the INFLO
   * algorithm.
   */
  public static final AssociationID<Double> INFLO_SCORE = AssociationID.getOrCreateAssociationID("inflo", Double.class);

  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public INFLO(Parameterization config) {
    super(config);
    if(config.grab(K_PARAM)) {
      k = K_PARAM.getValue();
    }
    if(config.grab(M_PARAM)) {
      m = M_PARAM.getValue();
    }
  }

  @Override
  protected MultiResult runInTime(Database<O> database) throws IllegalStateException {
    Set<Integer> processedIDs = new HashSet<Integer>(database.size());
    HashSet<Integer> pruned = new HashSet<Integer>();
    // KNNS
    HashMap<Integer, Vector<Integer>> knns = new HashMap<Integer, Vector<Integer>>();
    // RNNS
    HashMap<Integer, Vector<Integer>> rnns = new HashMap<Integer, Vector<Integer>>();
    // density
    HashMap<Integer, Double> density = new HashMap<Integer, Double>();
    // init knns and rnns
    for(Integer id : database) {
      knns.put(id, new Vector<Integer>());
      rnns.put(id, new Vector<Integer>());
    }

    for(Integer id : database) {
      // if not visited count=0
      int count = rnns.get(id).size();
      Vector<Integer> s;
      if(!processedIDs.contains(id)) {
        List<DistanceResultPair<DoubleDistance>> list = database.kNNQueryForID(id, k, getDistanceFunction());
        for(DistanceResultPair<DoubleDistance> d : list) {
          knns.get(id).add(d.second);

        }
        processedIDs.add(id);
        s = knns.get(id);
        density.put(id, 1 / list.get(k - 1).getDistance().doubleValue());

      }
      else {
        s = knns.get(id);
      }
      for(Integer q : s) {
        List<DistanceResultPair<DoubleDistance>> listQ;
        if(!processedIDs.contains(q)) {
          listQ = database.kNNQueryForID(q, k, getDistanceFunction());
          for(DistanceResultPair<DoubleDistance> dq : listQ) {
            knns.get(q).add(dq.second);
          }
          density.put(q, 1 / listQ.get(k - 1).getDistance().doubleValue());
          processedIDs.add(q);
        }

        if(knns.get(q).contains(id)) {
          rnns.get(q).add(id);
          rnns.get(id).add(q);
          count++;
        }
      }
      if(count >= s.size() * m) {
        pruned.add(id);
      }
    }

    // Calculate INFLO for any Object
    // IF Object is pruned INFLO=1.0
    MinMax<Double> inflominmax = new MinMax<Double>();
    HashMap<Integer, Double> inflos = new HashMap<Integer, Double>();
    for(Integer id : database) {
      if(!pruned.contains(id)) {
        Vector<Integer> knn = knns.get(id);
        Vector<Integer> rnn = rnns.get(id);

        double denP = density.get(id);
        knn.addAll(rnn);
        double den = 0;
        for(Integer q : knn) {
          double denQ = density.get(q);
          den = den + denQ;
        }
        den = den / rnn.size();
        den = den / denP;
        inflos.put(id, den);
        // update minimum and maximum
        inflominmax.put(den);

      }
      if(pruned.contains(id)) {
        inflos.put(id, 1.0);
        inflominmax.put(1.0);
      }
    }

    // Build result representation.
    AnnotationResult<Double> scoreResult = new AnnotationFromHashMap<Double>(INFLO_SCORE, inflos);
    OrderingResult orderingResult = new OrderingFromHashMap<Double>(inflos, true);
    OutlierScoreMeta scoreMeta = new QuotientOutlierScoreMeta(inflominmax.getMin(), inflominmax.getMax(), 0.0, Double.POSITIVE_INFINITY, 1.0);
    return new OutlierResult(scoreMeta, scoreResult, orderingResult);
  }
}