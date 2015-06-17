package de.lmu.ifi.dbs.elki.database;

import de.lmu.ifi.dbs.elki.data.DatabaseObject;
import de.lmu.ifi.dbs.elki.index.Index;

/**
 * IndexDatabase is a database implementation which is supported by an index
 * structure.
 * 
 * @author Elke Achtert
 * @param <O> the type of DatabaseObject as element of the database
 */
public abstract class IndexDatabase<O extends DatabaseObject> extends AbstractDatabase<O> {
  /**
   * Calls the super method and afterwards deletes the specified object from the
   * underlying index structure.
   */
  @Override
  public O delete(Integer id) {
    O object = super.delete(id);
    getIndex().delete(object);
    return object;
  }

  /**
   * Calls the super method and afterwards deletes the specified object from the
   * underlying index structure.
   */
  @Override
  public void delete(O object) {
    super.delete(object);
    getIndex().delete(object);
  }

  /**
   * Returns the physical read access of this database.
   * 
   * @return the physical read access of this database.
   */
  public long getPhysicalReadAccess() {
    return getIndex().getPhysicalReadAccess();
  }

  /**
   * Returns the physical write access of this database.
   * 
   * @return the physical write access of this database.
   */
  public long getPhysicalWriteReadAccess() {
    return getIndex().getPhysicalWriteAccess();
  }

  /**
   * Returns the logical page access of this database.
   * 
   * @return the logical page access of this database.
   */
  public long getLogicalPageAccess() {
    return getIndex().getLogicalPageAccess();
  }

  /**
   * Resets the page -access of this database.
   */
  public void resetPageAccess() {
    getIndex().resetPageAccess();
  }

  /**
   * Returns the underlying index structure.
   * 
   * @return the underlying index structure
   */
  public abstract Index<O> getIndex();
}