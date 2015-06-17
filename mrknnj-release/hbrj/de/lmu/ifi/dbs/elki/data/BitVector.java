package de.lmu.ifi.dbs.elki.data;

import de.lmu.ifi.dbs.elki.math.linearalgebra.Matrix;
import de.lmu.ifi.dbs.elki.math.linearalgebra.Vector;

import java.util.BitSet;
import java.util.List;
import java.util.Random;

/**
 * Provides a BitVector wrapping a BitSet.
 * 
 * @author Arthur Zimek
 */
public class BitVector extends AbstractNumberVector<BitVector, Bit> {
  /**
   * Storing the bits.
   */
  private BitSet bits;

  /**
   * Dimensionality of this bit vector.
   */
  private int dimensionality;

  /**
   * Provides a new BitVector corresponding to the specified bits and of the
   * specified dimensionality.
   * 
   * @param bits the bits to be set in this BitVector
   * @param dimensionality the dimensionality of this BitVector
   * @throws IllegalArgumentException if the specified dimensionality is to
   *         small to match the given BitSet
   */
  public BitVector(BitSet bits, int dimensionality) throws IllegalArgumentException {
    if(dimensionality < bits.length()) {
      throw new IllegalArgumentException("Specified dimensionality " + dimensionality + " is to low for specified BitSet of length " + bits.length());
    }
    this.bits = bits;
    this.dimensionality = dimensionality;
  }

  /**
   * Provides a new BitVector corresponding to the bits in the given array.
   * 
   * @param bits an array of bits specifying the bits in this bit vector
   */
  public BitVector(Bit[] bits) {
    this.bits = new BitSet(bits.length);
    for(int i = 0; i < bits.length; i++) {
      this.bits.set(i, bits[i].bitValue());
    }
    this.dimensionality = bits.length;
  }

  /**
   * Provides a new BitVector corresponding to the bits in the given list.
   * 
   * @param bits an array of bits specifying the bits in this bit vector
   */
  public BitVector(List<Bit> bits) {
    this.bits = new BitSet(bits.size());
    int i = 0;
    for(Bit bit : bits) {
      this.bits.set(i, bit.bitValue());
      i++;
    }
    this.dimensionality = bits.size();
  }

  @Override
  public BitVector newInstance(double[] values) {
    int dim = values.length;
    bits = new BitSet(dim);
    for (int i = 0; i < dim; i++) {
      if (values[i] >= 0.5) {
        bits.set(i);
      }
    }
    return new BitVector(bits, dim);
  }

  @Override
  public BitVector newInstance(Vector values) {
    int dim = values.getDimensionality();
    bits = new BitSet(dim);
    for (int i = 0; i < dim; i++) {
      if (values.get(i) >= 0.5) {
        bits.set(i);
      }
    }
    return new BitVector(bits, dim);
  }

  /**
   * Creates and returns a new BitVector based on the passed values.
   * 
   * @return a new instance of this BitVector with the specified values
   * 
   */
  @Override
  public BitVector newInstance(Bit[] values) {
    return new BitVector(values);
  }

  /**
   * Creates and returns a new BitVector based on the passed values.
   * 
   * @return a new instance of this BitVector with the specified values
   * 
   */
  @Override
  public BitVector newInstance(List<Bit> values) {
    return new BitVector(values);
  }

  /**
   * Returns a BitVector with random values.
   * 
   * @param random an instance of random to facilitate random values
   * @return a new instance of this BitVector with random values
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#randomInstance(java.util.Random)
   */
  public BitVector randomInstance(Random random) {
    Bit[] randomBits = new Bit[getDimensionality()];
    for(int i = 0; i < randomBits.length; i++) {
      randomBits[i] = new Bit(random.nextBoolean());
    }
    return new BitVector(randomBits);
  }

  /**
   * Returns the same as {@link BitVector#randomInstance(Random)
   * randomInstance(random)}.
   * 
   * @param min unused
   * @param max unused
   * @param random as in {@link BitVector#randomInstance(Random)
   *        randomInstance(random)}
   */
  public BitVector randomInstance(Bit min, Bit max, Random random) {
    return randomInstance(random);
  }

  /**
   * Returns the same as {@link BitVector#randomInstance(Random)
   * randomInstance(random)}.
   * 
   * @param min unused
   * @param max unused
   * @param random as in {@link BitVector#randomInstance(Random)
   *        randomInstance(random)}
   */
  public BitVector randomInstance(BitVector min, BitVector max, Random random) {
    return randomInstance(random);
  }

  /**
   * The dimensionality of the binary vector space of which this BitVector is an
   * element.
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#getDimensionality()
   */
  public int getDimensionality() {
    return dimensionality;
  }

  /**
   * Returns the value in the specified dimension.
   * 
   * @param dimension the desired dimension, where 1 &le; dimension &le;
   *        <code>this.getDimensionality()</code>
   * @return the value in the specified dimension
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#getValue(int)
   */
  public Bit getValue(int dimension) {
    if(dimension < 1 || dimension > dimensionality) {
      throw new IllegalArgumentException("illegal dimension: " + dimension);
    }
    return new Bit(bits.get(dimension));
  }

  /**
   * Returns the value in the specified dimension as double.
   * 
   * @param dimension the desired dimension, where 1 &le; dimension &le;
   *        <code>this.getDimensionality()</code>
   * @return the value in the specified dimension
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#doubleValue(int)
   */
  public double doubleValue(int dimension) {
    if(dimension < 1 || dimension > dimensionality) {
      throw new IllegalArgumentException("illegal dimension: " + dimension);
    }
    return bits.get(dimension) ? 1.0 : 0.0;
  }

  /**
   * Returns the value in the specified dimension as long.
   * 
   * @param dimension the desired dimension, where 1 &le; dimension &le;
   *        <code>this.getDimensionality()</code>
   * @return the value in the specified dimension
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#longValue(int)
   */
  public long longValue(int dimension) {
    if(dimension < 1 || dimension > dimensionality) {
      throw new IllegalArgumentException("illegal dimension: " + dimension);
    }
    return bits.get(dimension) ? 1 : 0;
  }

  /**
   * Returns a Vector representing in one column and
   * <code>getDimensionality()</code> rows the values of this BitVector as
   * double values.
   * 
   * @return a Matrix representing in one column and
   *         <code>getDimensionality()</code> rows the values of this BitVector
   *         as double values
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#getColumnVector()
   */
  public Vector getColumnVector() {
    double[] values = new double[dimensionality];
    for(int i = 0; i < dimensionality; i++) {
      values[i] = bits.get(i) ? 1 : 0;
    }
    return new Vector(values);
  }

  /**
   * Returns a Matrix representing in one row and
   * <code>getDimensionality()</code> columns the values of this BitVector as
   * double values.
   * 
   * @return a Matrix representing in one row and
   *         <code>getDimensionality()</code> columns the values of this
   *         BitVector as double values
   * 
   * @see de.lmu.ifi.dbs.elki.data.NumberVector#getRowVector()
   */
  public Matrix getRowVector() {
    double[] values = new double[dimensionality];
    for(int i = 0; i < dimensionality; i++) {
      values[i] = bits.get(i) ? 1 : 0;
    }
    return new Matrix(new double[][] { values.clone() });
  }

  /**
   * Returns a bit vector equal to this bit vector, if k is not 0, a bit vector
   * with all components equal to zero otherwise.
   * 
   * @param k used as multiplier 1 if k &ne; 0, otherwise the resulting bit
   *        vector will have all values equal to zero
   * @return a bit vector equal to this bit vector, if k is not 0, a bit vector
   *         with all components equal to zero otherwise
   */
  public BitVector multiplicate(double k) {
    if(k == 0) {
      return nullVector();
    }
    else {
      return new BitVector(bits, dimensionality);
    }
  }

  /**
   * Returns the inverse of the bit vector.
   * 
   * The result is the same as obtained by flipping all bits in the underlying
   * BitSet.
   * 
   * @return the inverse of the bit vector
   * @see BitSet#flip(int,int)
   */
  public BitVector negativeVector() {
    BitSet newBits = (BitSet) bits.clone();
    newBits.flip(0, dimensionality);
    return new BitVector(newBits, dimensionality);
  }

  /**
   * Returns a bit vector of equal dimensionality but containing 0 only.
   * 
   * @return a bit vector of equal dimensionality but containing 0 only
   */
  public BitVector nullVector() {
    return new BitVector(new BitSet(), dimensionality);
  }

  /**
   * Returns a bit vector corresponding to an XOR operation on this and the
   * specified bit vector.
   * 
   * @param fv the bit vector to add
   * @return a new bit vector corresponding to an XOR operation on this and the
   *         specified bit vector
   */
  public BitVector plus(BitVector fv) {
    if(this.getDimensionality()!=fv.getDimensionality()){
      throw new IllegalArgumentException("Incompatible dimensionality: " + this.getDimensionality() + " - " + fv.getDimensionality() + ".");
    }

    BitVector bv = new BitVector(fv.getBits(), this.dimensionality);
    bv.bits.xor(this.bits);
    return bv;
  }

  /**
   * Returns a bit vector corresponding to an NXOR operation on this and the
   * specified bit vector.
   * 
   * @param fv the bit vector to add
   * @return a new bit vector corresponding to an NXOR operation on this and the
   *         specified bit vector
   */
  public BitVector minus(BitVector fv) {
    if(this.getDimensionality()!=fv.getDimensionality()){
      throw new IllegalArgumentException("Incompatible dimensionality: " + this.getDimensionality() + " - " + fv.getDimensionality() + ".");
    }

    BitVector bv = new BitVector(fv.getBits(), this.dimensionality);
    bv.bits.flip(0, dimensionality);
    bv.bits.xor(this.bits);
    return bv;
  }

  /**
   * Returns whether the bit at specified index is set.
   * 
   * @param index the index of the bit to inspect
   * @return true if the bit at index <code>index</code> is set, false
   *         otherwise.
   */
  public boolean isSet(int index) {
    return bits.get(index);
  }

  /**
   * Returns whether the bits at all of the specified indices are set.
   * 
   * @param indices the indices to inspect
   * @return true if the bits at all of the specified indices are set, false
   *         otherwise
   */
  public boolean areSet(int[] indices) {
    boolean set = true;
    for(int i = 0; i < indices.length && set; i++) {
      // noinspection ConstantConditions
      set &= bits.get(i);
    }
    return set;
  }

  /**
   * Returns the indices of all set bits.
   * 
   * @return the indices of all set bits
   */
  public int[] setBits() {
    int[] setBits = new int[bits.size()];
    int index = 0;
    for(int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      setBits[index++] = i;
    }
    return setBits;
  }

  /**
   * Returns whether this BitVector contains all bits that are set to true in
   * the specified BitSet.
   * 
   * @param bitset the bits to inspect in this BitVector
   * @return true if this BitVector contains all bits that are set to true in
   *         the specified BitSet, false otherwise
   */
  public boolean contains(BitSet bitset) {
    boolean contains = true;
    for(int i = bitset.nextSetBit(0); i >= 0 && contains; i = bitset.nextSetBit(i + 1)) {
      // noinspection ConstantConditions
      contains &= bits.get(i);
    }
    return contains;
  }

  /**
   * Returns a copy of the bits currently set in this BitVector.
   * 
   * @return a copy of the bits currently set in this BitVector
   */
  public BitSet getBits() {
    return (BitSet) bits.clone();
  }

  /**
   * Returns a String representation of this BitVector. The representation is
   * suitable to be parsed by
   * {@link de.lmu.ifi.dbs.elki.parser.BitVectorLabelParser
   * BitVectorLabelParser}.
   * 
   * @see Object#toString()
   */
  @Override
  public String toString() {
    Bit[] bitArray = new Bit[dimensionality];
    for(int i = 0; i < dimensionality; i++) {
      bitArray[i] = new Bit(bits.get(i));
    }
    StringBuffer representation = new StringBuffer();
    for(Bit bit : bitArray) {
      if(representation.length() > 0) {
        representation.append(ATTRIBUTE_SEPARATOR);
      }
      representation.append(bit.toString());
    }
    return representation.toString();
  }

  /**
   * Indicates whether some other object is "equal to" this BitVector. This
   * BitVector is equal to the given object, if the object is a BitVector of
   * same dimensionality and with identical bits set.
   * 
   * @see DatabaseObject#equals(Object)
   */
  @Override
  public boolean equals(Object obj) {
    if(obj instanceof BitVector) {
      BitVector bv = (BitVector) obj;
      return this.getDimensionality() == bv.getDimensionality() && this.getBits().equals(bv.getBits());

    }
    else {
      return false;
    }
  }

  /**
   * Provides the scalar product (inner product) of this BitVector and the given BitVector.
   * 
   * As multiplication of Bits, the logical AND operation is used.
   * The result is 0 if the number of bits after the AND operation is a multiple of 2, otherwise the result is 1.
   * 
   * @param fv the BitVector to compute the scalar product for
   * @return the scalar product (inner product) of this and the given BitVector
   */
  @Override
  public Bit scalarProduct(BitVector fv) {
    if(this.getDimensionality()!=fv.getDimensionality()){
      throw new IllegalArgumentException("Incompatible dimensionality: " + this.getDimensionality() + " - " + fv.getDimensionality() + ".");
    }
    BitSet bs = this.getBits();
    bs.and(fv.bits);

    return new Bit(bs.cardinality()%2==1);
  }

}
