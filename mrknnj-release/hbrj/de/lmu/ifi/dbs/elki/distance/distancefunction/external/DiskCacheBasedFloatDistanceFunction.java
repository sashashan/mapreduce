package de.lmu.ifi.dbs.elki.distance.distancefunction.external;

import java.io.File;
import java.io.IOException;

import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.distance.FloatDistance;
import de.lmu.ifi.dbs.elki.distance.distancefunction.AbstractDistanceFunction;
import de.lmu.ifi.dbs.elki.persistent.OnDiskUpperTriangleMatrix;
import de.lmu.ifi.dbs.elki.utilities.ByteArrayUtil;
import de.lmu.ifi.dbs.elki.utilities.documentation.Description;
import de.lmu.ifi.dbs.elki.utilities.documentation.Title;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.OptionID;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.WrongParameterValueException;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameters.FileParameter;

/**
 * Provides a DistanceFunction that is based on float distances given by a
 * distance matrix of an external file.
 * 
 * @author Erich Schubert
 * @param <V> object type
 */
@Title("File based float distance for database objects.")
@Description("Loads float distance values from an external matrix.")
public class DiskCacheBasedFloatDistanceFunction<V extends DatabaseObject> extends AbstractDistanceFunction<V, FloatDistance> implements Parameterizable {
  /**
   * Magic to identify double cache matrices
   */
  public static final int FLOAT_CACHE_MAGIC = 23423411;

  /**
   * OptionID for {@link #MATRIX_PARAM}
   */
  public static final OptionID MATRIX_ID = OptionID.getOrCreateOptionID("distance.matrix", "The name of the file containing the distance matrix.");

  /**
   * Storage required for a float value.
   */
  private static final int FLOAT_SIZE = 4;

  /**
   * Parameter that specifies the name of the directory to be re-parsed.
   * <p>
   * Key: {@code -distance.matrix}
   * </p>
   */
  private final FileParameter MATRIX_PARAM = new FileParameter(MATRIX_ID, FileParameter.FileType.INPUT_FILE);

  private OnDiskUpperTriangleMatrix cache = null;
  
  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public DiskCacheBasedFloatDistanceFunction(Parameterization config) {
    super(new FloatDistance());
    if (config.grab(MATRIX_PARAM)) {
      File matrixfile = MATRIX_PARAM.getValue();

      try {
        cache = new OnDiskUpperTriangleMatrix(matrixfile,FLOAT_CACHE_MAGIC,0,FLOAT_SIZE,false);
      }
      catch(IOException e) {
        config.reportError(new WrongParameterValueException(MATRIX_PARAM, matrixfile.toString(), e));      
      }      
    }
  }

  /**
   * Computes the distance between two given DatabaseObjects according to this
   * distance function.
   * 
   * @param o1 first DatabaseObject
   * @param o2 second DatabaseObject
   * @return the distance between two given DatabaseObject according to this
   *         distance function
   */
  public FloatDistance distance(V o1, V o2) {
    return distance(o1.getID(), o2.getID());
  }

  /**
   * Returns the distance between the two specified objects.
   * 
   * @param id1 first object id
   * @param o2 second DatabaseObject
   * @return the distance between the two objects specified by their objects ids
   */
  @Override
  public FloatDistance distance(Integer id1, V o2) {
    return distance(id1, o2.getID());
  }

  /**
   * Returns the distance between the two objects specified by their objects
   * ids. If a cache is used, the distance value is looked up in the cache. If
   * the distance does not yet exists in cache, it will be computed an put to
   * cache. If no cache is used, the distance is computed.
   * 
   * @param id1 first object id
   * @param id2 second object id
   * @return the distance between the two objects specified by their objects ids
   */
  @Override
  public FloatDistance distance(Integer id1, Integer id2) {
    if (id1 == null) {
      return undefinedDistance();
    }
    if (id2 == null) {
      return undefinedDistance();
    }
    // the smaller id is the first key
    if (id1 > id2) {
      return distance(id2, id1);
    }

    float distance;
    try {
      byte[] data = cache.readRecord(id1, id2);
      distance = ByteArrayUtil.readFloat(data,0);
    }
    catch(IOException e) {
      throw new RuntimeException("Read error when loading distance "+id1+","+id2+" from cache file.", e);
    }
    return new FloatDistance(distance);
  }
}
