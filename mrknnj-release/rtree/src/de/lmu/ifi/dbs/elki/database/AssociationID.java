package de.lmu.ifi.dbs.elki.database;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.SortedSet;

import de.lmu.ifi.dbs.elki.data.ClassLabel;
import de.lmu.ifi.dbs.elki.distance.similarityfunction.kernel.KernelMatrix;
import de.lmu.ifi.dbs.elki.math.linearalgebra.Matrix;
import de.lmu.ifi.dbs.elki.math.linearalgebra.pca.PCAFilteredResult;
import de.lmu.ifi.dbs.elki.utilities.ConstantObject;

/**
 * An AssociationID is used by databases as a unique identifier for specific
 * associations to single objects. Such as label, local similarity measure.
 * There is no association possible without a specific AssociationID defined
 * within this class.
 * <p/>
 * An AssociationID provides also information concerning the class of the
 * associated objects.
 * 
 * @author Arthur Zimek
 * @param <C> the type of the class of the associated object
 */
public class AssociationID<C> extends ConstantObject<AssociationID<C>> {
  /**
   * The standard association id to associate a label to an object.
   */
  public static final AssociationID<String> LABEL = new AssociationID<String>("label", String.class);

  /**
   * The association id to associate a class (class label) to an object.
   */
  public static final AssociationID<ClassLabel> CLASS = new AssociationID<ClassLabel>("class", ClassLabel.class);

  /**
   * The association id to associate an external id to an object.
   */
  public static final AssociationID<String> EXTERNAL_ID = new AssociationID<String>("externalID", String.class);

  /**
   * The association id to associate a row id to an object.
   */
  public static final AssociationID<Integer> ROW_ID = new AssociationID<Integer>("rowID", Integer.class);

  /**
   * The association id to associate a correlation pca to an object.
   */
  public static final AssociationID<PCAFilteredResult> LOCAL_PCA = new AssociationID<PCAFilteredResult>("pca", PCAFilteredResult.class);

  /**
   * The association id to associate a local dimensionality (e.g. the
   * correlation dimensionality) to an object.
   */
  public static final AssociationID<Integer> LOCAL_DIMENSIONALITY = new AssociationID<Integer>("localDimensionality", Integer.class);

  /**
   * The association id to associate the neighbors of an object.
   */
  public static final AssociationID<List<Integer>> NEIGHBOR_IDS = new AssociationID<List<Integer>>("neighborids", List.class);

  /**
   * The association id to associate a set of neighbors for use of the shared
   * nearest neighbor similarity function.
   */
  public static final AssociationID<SortedSet<Integer>> SHARED_NEAREST_NEIGHBORS_SET = new AssociationID<SortedSet<Integer>>("sharedNearestNeighborList", SortedSet.class);

  /**
   * The association id to associate a set of neighbors for use of the shared
   * nearest neighbor similarity function.
   */
  public static final AssociationID<ArrayList<Integer>> RANKING_LIST = new AssociationID<ArrayList<Integer>>("rankingList", ArrayList.class);

  /**
   * The association id to associate the locally weighted matrix of an object
   * for the locally weighted distance function.
   */
  public static final AssociationID<Matrix> LOCALLY_WEIGHTED_MATRIX = new AssociationID<Matrix>("locallyWeightedMatrix", Matrix.class);

  /**
   * The association id to associate a preference vector.
   */
  public static final AssociationID<BitSet> PREFERENCE_VECTOR = new AssociationID<BitSet>("preferenceVector", BitSet.class);

  /**
   * The association id to associate the strong eigenvector weighted matrix of
   * an object.
   */
  public static final AssociationID<Matrix> STRONG_EIGENVECTOR_MATRIX = new AssociationID<Matrix>("strongEigenvectorMatrix", Matrix.class);

  /**
   * The association id to associate an arbitrary matrix of an object.
   */
  public static final AssociationID<Matrix> CACHED_MATRIX = new AssociationID<Matrix>("cachedMatrix", Matrix.class);

  /**
   * The association id to associate a kernel matrix.
   */
  public static final AssociationID<KernelMatrix<?>> KERNEL_MATRIX = new AssociationID<KernelMatrix<?>>("kernelMatrix", KernelMatrix.class);

  /**
   * The serial version UID.
   */
  private static final long serialVersionUID = 8115554038339292192L;

  /**
   * The Class type related to this AssociationID.
   */
  private Class<C> type;

  /**
   * Provides a new AssociationID of the given name and type.
   * <p/>
   * All AssociationIDs are unique w.r.t. their name. An AssociationID provides
   * information of which class the associated objects are.
   * 
   * @param name name of the association
   * @param type class of the objects that are associated under this
   *        AssociationID
   */
  @SuppressWarnings("unchecked")
  private AssociationID(final String name, final Class<?> type) {
    // It's more useful to use Class<?> here to allow the use of nested
    // Generics such as List<Foo<Bar>>
    super(name);
    try {
      this.type = (Class<C>) Class.forName(type.getName());
    }
    catch(ClassNotFoundException e) {
      throw new IllegalArgumentException("Invalid class name \"" + type.getName() + "\" for property \"" + name + "\".");
    }
  }

  /**
   * Returns the type of the AssociationID.
   * 
   * @return the type of the AssociationID
   */
  @SuppressWarnings("unchecked")
  public Class<C> getType() {
    try {
      return (Class<C>) Class.forName(type.getName());
    }
    catch(ClassNotFoundException e) {
      throw new IllegalStateException("Invalid class name \"" + type.getName() + "\" for property \"" + this.getName() + "\".");
    }
  }

  /**
   * Returns the AssociationID for the given name if it exists, null otherwise.
   * 
   * @param name the name of the desired AssociationID
   * @return the AssociationID for the given name if it exists, null otherwise
   */
  // We extensively suppress warnings because of compiler differences in what
  // warning they generate here - including "unneeded suppressWarnings". Argh.
  @SuppressWarnings( { "unchecked", "cast", "all" })
  public static AssociationID<?> getAssociationID(final String name) {
    return (AssociationID<?>) AssociationID.lookup(AssociationID.class, name);
  }

  /**
   * Gets or creates the AssociationID for the given name and given type.
   * 
   * @param <C> association class
   * @param name the name
   * @param type the type of the association
   * @return the AssociationID for the given name
   */
  @SuppressWarnings("unchecked")
  public static <C> AssociationID<C> getOrCreateAssociationID(final String name, final Class<C> type) {
    AssociationID<C> associationID = (AssociationID<C>) getAssociationID(name);
    if(associationID == null) {
      associationID = new AssociationID<C>(name, type);
    }
    return associationID;
  }

  /**
   * Gets or creates the AssociationID for the given name and given type.
   * Generics version, with relaxed typechecking.
   * 
   * @param <C> association class
   * @param name the name
   * @param type the type of the association
   * @return the AssociationID for the given name
   */
  @SuppressWarnings("unchecked")
  public static <C> AssociationID<C> getOrCreateAssociationIDGenerics(final String name, final Class<?> type) {
    AssociationID<C> associationID = (AssociationID<C>) getAssociationID(name);
    if(associationID == null) {
      associationID = new AssociationID<C>(name, type);
    }
    return associationID;
  }

  /**
   * Return the name formatted for use in text serialization
   * 
   * @return uppercased, no whitespace version of the association name.
   */
  public String getLabel() {
    return getName().replace(" ", "_").toUpperCase();
  }
}
