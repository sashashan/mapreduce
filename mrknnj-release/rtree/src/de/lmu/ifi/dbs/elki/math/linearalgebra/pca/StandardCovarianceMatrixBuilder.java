package de.lmu.ifi.dbs.elki.math.linearalgebra.pca;

import java.util.Collection;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.database.Database;
import de.lmu.ifi.dbs.elki.distance.NumberDistance;
import de.lmu.ifi.dbs.elki.math.linearalgebra.Matrix;
import de.lmu.ifi.dbs.elki.utilities.DatabaseUtil;

/**
 * Class for building a "traditional" covariance matrix.
 * Reasonable default choice for a {@link CovarianceMatrixBuilder}
 * 
 * @author Erich Schubert
 *
 * @param <V> Vector class to use.
 * @param <D> Distance type
 */
public class StandardCovarianceMatrixBuilder<V extends NumberVector<V, ?>, D extends NumberDistance<D,?>> extends CovarianceMatrixBuilder<V,D> {
  /**
   * Compute Covariance Matrix for a complete database
   * 
   * @param database the database used
   * @return Covariance Matrix
   */
  @Override
  public Matrix processDatabase(Database<V> database) {
    return DatabaseUtil.covarianceMatrix(database);
  }

  /**
   * Compute Covariance Matrix for a collection of database IDs
   * 
   * @param ids a collection of ids
   * @param database the database used
   * @return Covariance Matrix
   */
  @Override
  public Matrix processIds(Collection<Integer> ids, Database<V> database) {
    return DatabaseUtil.covarianceMatrix(database, ids);
  }
}