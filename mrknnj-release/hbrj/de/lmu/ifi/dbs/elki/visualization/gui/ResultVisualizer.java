package de.lmu.ifi.dbs.elki.visualization.gui;

import java.util.Collection;

import javax.swing.JFrame;

import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.logging.Logging;
import de.lmu.ifi.dbs.elki.normalization.Normalization;
import de.lmu.ifi.dbs.elki.result.MultiResult;
import de.lmu.ifi.dbs.elki.result.Result;
import de.lmu.ifi.dbs.elki.result.ResultHandler;
import de.lmu.ifi.dbs.elki.result.ResultUtil;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.constraints.GreaterEqualConstraint;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.IntParameter;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.StringParameter;
import de.lmu.ifi.dbs.elki.visualization.gui.overview.OverviewPlot;
import de.lmu.ifi.dbs.elki.visualization.visualizers.Visualizer;
import de.lmu.ifi.dbs.elki.visualization.visualizers.VisualizersForResult;

/**
 * Handler to process and visualize a Result.
 * 
 * @author Erich Schubert
 * @author Remigius Wojdanowski
 */
public class ResultVisualizer implements ResultHandler<DatabaseObject, Result> {
  /**
   * Get a logger for this class.
   */
  protected final static Logging logger = Logging.getLogger(ResultVisualizer.class);

  /**
   * OptionID for {@link #WINDOW_TITLE_PARAM}
   */
  public static final OptionID WINDOW_TITLE_ID = OptionID.getOrCreateOptionID("vis.window.title", "Title to use for visualization window.");

  /**
   * Parameter to specify the window title
   * <p>
   * Key: {@code -vis.window.title}
   * </p>
   * <p>
   * Default value: "ELKI Result Visualization"
   * </p>
   */
  protected final StringParameter WINDOW_TITLE_PARAM = new StringParameter(WINDOW_TITLE_ID, true);
  
  /**
   * Stores the set title.
   */
  private String title;
  
  /**
   * Default title
   */
  protected final static String DEFAULT_TITLE = "ELKI Result Visualization";

  /**
   * OptionID for {@link #MAXDIM_PARAM}.
   */
  public static final OptionID MAXDIM_ID = OptionID.getOrCreateOptionID("vis.maxdim", "Maximum number of dimensions to display.");
  
  /**
   * Parameter for the maximum number of dimensions,
   * 
   * <p>
   * Code: -vis.maxdim
   * </p>
   */
  private IntParameter MAXDIM_PARAM = new IntParameter(MAXDIM_ID, new GreaterEqualConstraint(1), OverviewPlot.MAX_DIMENSIONS_DEFAULT);

  /**
   * Stores the maximum number of dimensions to show.
   */
  private int maxdim = OverviewPlot.MAX_DIMENSIONS_DEFAULT;

  /**
   * Visualization manager.
   */
  VisualizersForResult manager;
  
  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public ResultVisualizer(Parameterization config) {
    super();
    if (config.grab(WINDOW_TITLE_PARAM)) {
      title = WINDOW_TITLE_PARAM.getValue();
    }
    if (config.grab(MAXDIM_PARAM)) {
      maxdim  = MAXDIM_PARAM.getValue();
    }
    manager = new VisualizersForResult(config);
  }

  @Override
  public void processResult(Database<DatabaseObject> db, Result result) {
    MultiResult mr = ResultUtil.ensureMultiResult(result);
    manager.processResult(db, mr);
    Collection<Visualizer> vs = manager.getVisualizers();
    if (vs.size() == 0) {
      logger.error("No visualizers found for result!");
      return;
    }
    
    if (title == null) {
      title = manager.getTitle(db, mr);
    }
    
    if (title == null) {
      title = DEFAULT_TITLE;
    }
    
    ResultWindow window = new ResultWindow(title, db, mr, maxdim);
    window.addVisualizations(vs);
    window.setVisible(true);
    window.setExtendedState(window.getExtendedState() | JFrame.MAXIMIZED_BOTH);    
  }

  @Override
  public void setNormalization(@SuppressWarnings("unused") Normalization<DatabaseObject> normalization) {
    // TODO: handle normalizations
    logger.warning("Normalizations not yet supported in " + ResultVisualizer.class.getName());
  }
}
