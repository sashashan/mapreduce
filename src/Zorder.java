//package de.lmu.ifi.dbs.elki.index;

import java.io.*;
import java.util.*;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;
import java.math.*;

public class Zorder {
	// Pad a String 
	public static String createExtra(int num) 
	{
		if( num < 1 ) return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++) 
			extra[i] = '0';
		return (new String(extra)); 	
	}

/*
	public static int[] createShift(int dimension, Random rin, boolean shift) 
	{
		Random r = rin; 
		int[] rv = new int[dimension];  // random vector 

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = ((int) Math.abs(r.nextInt())); 
				// System.out.printf("%d ", rv[i]);
			}	
			//System.out.println();
		} else {
			for (int i = 0; i < dimension; i++)	
				rv[i] = 0;
		}

		return rv;
	}
*/

	public static int maxDecDigits( int dimension ) 
	{
		System.out.println("In madDecDigits");
		int max = 32;
		BigInteger maxDec = new BigInteger( "1" );
		maxDec = maxDec.shiftLeft( dimension * max );
		//System.out.println("maxDec: " + Integer.toBinaryString(maxDec));
		maxDec.subtract( BigInteger.ONE );
		//System.out.println("maxDec: " + Integer.toBinaryString(maxDec));
		return maxDec.toString().length();
	}

	public static String maxDecString( int dimension ) 
	{
		int max = 32;
		BigInteger maxDec = new BigInteger( "1" );
		maxDec = maxDec.shiftLeft( dimension * max );
		maxDec.subtract( BigInteger.ONE );
		return maxDec.toString();
	}
	
	// Convert an multi-dimensional coordinate into a zorder
	// coordinates have already been scaled and shifted
	public static String valueOf(int dimension, int[] coord) 
	{
		Vector<String> arrPtr = new Vector<String>(dimension);
		//System.out.println( "maxDec " + maxDec.toString() );
        int max = 32;
		int fix = maxDecDigits(dimension); //global maximum possible zvalue length
		System.out.println("fix: " + fix );

		for (int i = 0; i < dimension; i++) {
			String p = Integer.toBinaryString((int)coord[i]);
			System.out.println( coord[i] + " " + p ); 
			arrPtr.add(p);
		}

		for( int i = 0; i < arrPtr.size(); ++i ) {
			String extra = createExtra( max - arrPtr.elementAt(i).length() ); 
			arrPtr.set(i, extra + arrPtr.elementAt(i) );
			System.out.println( i + " " + arrPtr.elementAt(i) );
		}
	
		char[] value = new char[dimension * max];
		int index = 0;

		// Create Zorder
		for (int i = 0; i < max; ++i ) {
			for (String e: arrPtr) {
				char ch = e.charAt(i);
				value[index++] = ch;
			}	
		}		
			
		String order = new String(value);
		//System.out.println( value );
		// Covert a binary representation of order into a big integer
		BigInteger ret = new BigInteger( order, 2 );

		// Return a fixed length decimal String representation of 
		// the big integer (z-order)
		order = ret.toString();	
		//System.out.println( order );
		if (order.length() < fix) {
			String extra = createExtra(fix - order.length());
			order = extra + order;
		} else if (order.length() > fix) {
			System.out.println("too big zorder, need to fix Zorder.java");
			System.exit(-1);
		}

		//System.out.println(order);
		
		return order;
	}

	//update on 11/24/2010 by cz
	//update 11.29.2010 by jeff
	public static int[] toCoord(String z, int dimension) 
	{
		int DECIMAL_RADIX = 10;
		int BINARY_RADIX = 2;

		if (z == null) {
			System.out.println("Z-order Null pointer!!!@Zorder.toCoord");
			System.exit(-1);	
		}

		BigInteger bigZ = new BigInteger(z, DECIMAL_RADIX);
		String bigZStr = bigZ.toString(BINARY_RADIX);

		// Test
		//bigZStr = "1110011";
		//System.out.println(bigZStr);

		int len = bigZStr.length();
		// System.out.println("leng before is" + len);
		//int prefixZeros = len % dimension;
		int prefixZeros = 0;
		if (len % dimension != 0)
			prefixZeros = dimension - len % dimension;

		//System.out.println("--");
		//System.out.println(prefixZeros);

		String prefix = Zorder.createExtra(prefixZeros);
		bigZStr = prefix + bigZStr;
		
		len = bigZStr.length();
		//System.out.println(len);

		if (len % dimension != 0) {
			System.out.println("Wrong prefix!!!@Zorder.toCoord");
			System.exit(-1);
		}

		//The most significant bit is save at starting position of 
		//the char array.
		char[] bigZCharArray = bigZStr.toCharArray();

		int[] coord = new int[dimension];
		for (int i = 0; i < dimension; i++)
			coord[i] = 0;

		for( int i = 0; i < bigZCharArray.length; ) 
		{
			for( int j = 0; j < dimension; ++j ) 
			{
				coord[j] <<= 1;
				coord[j] |= bigZCharArray[i++] - '0';
   			}
		}

		return coord;
	}

	public static void main(String[] args) 
	{
		//39886296 12356.6276 27642.1062 Hawaii,?HI

		int dimension = 2;
		int coordOffset = 1;
		String[] parts = new String [3];
		int scale = 1000;

		parts[0] = "39886296"; // ID
		parts[1] = "12356.6276";
		parts[2] = "27642.1062";

		String recId = parts[0];
		int recIdInt = Integer.parseInt(recId);
		float[] coord = new float[dimension];

		for (int i = 0; i < dimension; i++) {
			coord[i] = Float.parseFloat(parts[coordOffset + i]);
			System.out.println("Coord[i] " + coord[i]);	
		}

		int[] converted_coord = new int[dimension];

		for (int i = 0; i < dimension; i++)	
		{
			converted_coord[i] = (int) coord[i];        // get the integer part
			System.out.println("(integer) converted_coord[" + i + "]: " + converted_coord[i]);
			coord[i] = coord[i] - converted_coord[i];   // get the fraction part
			System.out.println("(fraction) coord[" + i + "]: " + coord[i]);
			converted_coord[i] *= scale;                // scale integer part
			System.out.println("(scaled integer) converted_coord[" + i + "]: " + converted_coord[i]);
			converted_coord[i] += coord[i] * scale;     // scale fraction part
			System.out.println("(scaled fraction) converted_coord[" + i + "]: " + converted_coord[i]);
		}
		
		String zval = Zorder.valueOf(dimension, converted_coord); 
	}
}
