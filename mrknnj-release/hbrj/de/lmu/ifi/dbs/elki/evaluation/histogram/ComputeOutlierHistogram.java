package de.lmu.ifi.dbs.elki.evaluation.histogram;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import de.lmu.ifi.dbs.elki.algorithm.AbstractAlgorithm;
import de.lmu.ifi.dbs.elki.algorithm.Algorithm;
import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.data.DoubleVector;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.distance.distancefunction.EuclideanDistanceFunction;
import de.lmu.ifi.dbs.elki.math.AggregatingHistogram;
import de.lmu.ifi.dbs.elki.math.FlexiHistogram;
import de.lmu.ifi.dbs.elki.result.HistogramResult;
import de.lmu.ifi.dbs.elki.result.MultiResult;
import de.lmu.ifi.dbs.elki.result.Result;
import de.lmu.ifi.dbs.elki.result.ResultUtil;
import de.lmu.ifi.dbs.elki.result.outlier.OutlierResult;
import de.lmu.ifi.dbs.elki.utilities.DatabaseUtil;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.Flag;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.ObjectParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.PatternParameter;
import de.lmu.ifi.dbs.elki.utilities.pairs.Pair;
import de.lmu.ifi.dbs.elki.utilities.scaling.IdentityScaling;
import de.lmu.ifi.dbs.elki.utilities.scaling.ScalingFunction;
import de.lmu.ifi.dbs.elki.utilities.scaling.outlier.OutlierScalingFunction;

/**
 * Compute a Histogram to evaluate a ranking algorithm.
 * 
 * The parameter {@code -hist.positive} specifies the class label of "positive"
 * hits.
 * 
 * @author Lisa Reichert
 * @author Erich Schubert
 * 
 * @param <O> Database object type
 */
public class ComputeOutlierHistogram<O extends DatabaseObject> extends AbstractAlgorithm<O, MultiResult> {
  /**
   * OptionID for {@link #POSITIVE_CLASS_NAME_PARAM}
   */
  public static final OptionID POSITIVE_CLASS_NAME_ID = OptionID.getOrCreateOptionID("comphist.positive", "Class label for the 'positive' class.");

  /**
   * OptionID for {@link #BINS_ID}
   */
  public static final OptionID BINS_ID = OptionID.getOrCreateOptionID("comphist.bins", "number of bins");

  /**
   * OptionID for {@link #SCALING_PARAM}
   */
  public static final OptionID SCALING_ID = OptionID.getOrCreateOptionID("comphist.scaling", "Class to use as scaling function.");

  /**
   * OptionID for {@link #SPLITFREQ_PARAM}
   */
  public static final OptionID SPLITFREQ_ID = OptionID.getOrCreateOptionID("histogram.splitfreq", "Use separate frequencies for outliers and non-outliers.");

  /**
   * The object pattern to identify positive classes
   * <p>
   * Key: {@code -comphist.positive}
   * </p>
   */
  private final PatternParameter POSITIVE_CLASS_NAME_PARAM = new PatternParameter(POSITIVE_CLASS_NAME_ID, true);

  /**
   * number of bins for the histogram
   * <p>
   * Default value: {@link EuclideanDistanceFunction}
   * </p>
   * <p>
   * Key: {@code -comphist.bins}
   * </p>
   */
  private final IntParameter BINS_PARAM = new IntParameter(BINS_ID);

  /**
   * Parameter to specify the algorithm to be applied, must extend
   * {@link de.lmu.ifi.dbs.elki.algorithm.Algorithm}.
   * <p>
   * Key: {@code -algorithm}
   * </p>
   */
  private final ObjectParameter<Algorithm<O, Result>> ALGORITHM_PARAM = new ObjectParameter<Algorithm<O, Result>>(OptionID.ALGORITHM, Algorithm.class);

  /**
   * Parameter to specify a scaling function to use.
   * <p>
   * Key: {@code -comphist.scaling}
   * </p>
   */
  private final ObjectParameter<ScalingFunction> SCALING_PARAM = new ObjectParameter<ScalingFunction>(SCALING_ID, ScalingFunction.class, IdentityScaling.class);

  /**
   * Flag to count frequencies of outliers and non-outliers separately
   * <p>
   * Key: {@code -histogram.splitfreq}
   * </p>
   */
  private final Flag SPLITFREQ_PARAM = new Flag(SPLITFREQ_ID);

  /**
   * Stores the "positive" class.
   */
  private Pattern positive_class_name = null;

  /**
   * Number of bins
   */
  private int bins;

  /**
   * Holds the algorithm to run.
   */
  private Algorithm<O, Result> algorithm;

  /**
   * Scaling function to use
   */
  private ScalingFunction scaling;

  /**
   * Flag to make split frequencies
   */
  private boolean splitfreq = false;

  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public ComputeOutlierHistogram(Parameterization config) {
    super(config);
    if(config.grab(POSITIVE_CLASS_NAME_PARAM)) {
      positive_class_name = POSITIVE_CLASS_NAME_PARAM.getValue();
    }
    if(config.grab(ALGORITHM_PARAM)) {
      algorithm = ALGORITHM_PARAM.instantiateClass(config);
    }
    if(config.grab(BINS_PARAM)) {
      bins = BINS_PARAM.getValue();
    }
    if(config.grab(SCALING_PARAM)) {
      scaling = SCALING_PARAM.instantiateClass(config);
    }
    if(config.grab(SPLITFREQ_PARAM)) {
      splitfreq = SPLITFREQ_PARAM.getValue();
    }
  }

  @Override
  protected MultiResult runInTime(Database<O> database) throws IllegalStateException {
    Result innerresult = algorithm.run(database);

    OutlierResult or = getOutlierResult(database, innerresult);
    if(scaling instanceof OutlierScalingFunction) {
      OutlierScalingFunction oscaling = (OutlierScalingFunction) scaling;
      oscaling.prepare(database, innerresult, or);
    }

    Collection<Integer> ids = database.getIDs();
    Collection<Integer> outlierIds = DatabaseUtil.getObjectsByLabelMatch(database, positive_class_name);
    // first value for outliers, second for each object
    final AggregatingHistogram<Pair<Double, Double>, Pair<Double, Double>> hist;
    // If we have useful (finite) min/max, use these for binning.
    double min = scaling.getMin();
    double max = scaling.getMax();
    if(Double.isInfinite(min) || Double.isNaN(min) || Double.isInfinite(max) || Double.isNaN(max)) {
      hist = FlexiHistogram.DoubleSumDoubleSumHistogram(bins);
    }
    else {
      hist = AggregatingHistogram.DoubleSumDoubleSumHistogram(bins, min, max);
    }
    // first fill histogram only with values of outliers
    Pair<Double, Double> positive, negative;
    if(!splitfreq) {
      positive = new Pair<Double, Double>(0., 1. / ids.size());
      negative = new Pair<Double, Double>(1. / ids.size(), 0.);
    }
    else {
      positive = new Pair<Double, Double>(0., 1. / outlierIds.size());
      negative = new Pair<Double, Double>(1. / (ids.size() - outlierIds.size()), 0.);
    }
    ids.removeAll(outlierIds);
    // fill histogram with values of each object
    for(Integer id : ids) {
      double result = or.getScores().getValueFor(id);
      result = scaling.getScaled(result);
      hist.aggregate(result, negative);
    }
    for(Integer id : outlierIds) {
      double result = or.getScores().getValueFor(id);
      result = scaling.getScaled(result);
      hist.aggregate(result, positive);
    }

    // turn into Collection

    Collection<DoubleVector> collHist = new ArrayList<DoubleVector>(hist.getNumBins());
    for(Pair<Double, Pair<Double, Double>> ppair : hist) {
      Pair<Double, Double> data = ppair.getSecond();
      DoubleVector row = new DoubleVector(new double[] { ppair.getFirst(), data.getFirst(), data.getSecond() });
      collHist.add(row);
    }

    MultiResult result = new MultiResult();
    result.addResult(innerresult);
    result.addResult(new HistogramResult<DoubleVector>(collHist));

    return result;
  }

  /**
   * Find an OutlierResult to work with.
   * 
   * @param database Database context
   * @param result Result object
   * @return Iterator to work with
   */
  private OutlierResult getOutlierResult(Database<O> database, Result result) {
    List<OutlierResult> ors = ResultUtil.filterResults(result, OutlierResult.class);
    if(ors.size() > 0) {
      return ors.get(0);
    }
    throw new IllegalStateException("Comparison algorithm expected at least one outlier result.");
  }
}