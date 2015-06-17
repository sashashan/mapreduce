package de.lmu.ifi.dbs.elki.index.tree.spatial.rstarvariants;

import java.util.ArrayList;
import java.util.List;

import de.lmu.ifi.dbs.elki.data.NumberVector;
import de.lmu.ifi.dbs.elki.data.FloatVector;
import de.lmu.ifi.dbs.elki.data.DoubleVector;
import de.lmu.ifi.dbs.elki.index.tree.spatial.BulkSplit;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialEntry;
import de.lmu.ifi.dbs.elki.index.tree.spatial.SpatialObject;
import de.lmu.ifi.dbs.elki.utilities.optionhandling.parameterization.Parameterization;

import java.io.*; // CZ
import de.lmu.ifi.dbs.elki.index.tree.spatial.BulkSplit;
import de.lmu.ifi.dbs.elki.persistent.PageFile;
import de.lmu.ifi.dbs.elki.persistent.PersistentPageFile;
import de.lmu.ifi.dbs.elki.index.Zorder;
import de.lmu.ifi.dbs.elki.index.ExternalSort;
/**
 * Abstract superclass for all non-flat R*-Tree variants.
 * 
 * @author Elke Achtert
 * @param <O> Object type
 * @param <N> Node type
 * @param <E> Entry type
 */
public abstract class NonFlatRStarTree<O extends NumberVector<O, ?>, N extends AbstractRStarTreeNode<N, E>, E extends SpatialEntry> extends AbstractRStarTree<O, N, E> {
  /**
   * Constructor, adhering to
   * {@link de.lmu.ifi.dbs.elki.utilities.optionhandling.Parameterizable}
   * 
   * @param config Parameterization
   */
  public NonFlatRStarTree(Parameterization config) {
    super(config);
  }

  /**
   * Returns true if in the specified node an overflow occurred, false
   * otherwise.
   * 
   * @param node the node to be tested for overflow
   * @return true if in the specified node an overflow occurred, false otherwise
   */
  @Override
  protected boolean hasOverflow(N node) {
    if(node.isLeaf()) {
      return node.getNumEntries() == leafCapacity;
    }
    else {
      return node.getNumEntries() == dirCapacity;
    }
  }

  /**
   * Returns true if in the specified node an underflow occurred, false
   * otherwise.
   * 
   * @param node the node to be tested for underflow
   * @return true if in the specified node an underflow occurred, false
   *         otherwise
   */
  @Override
  protected boolean hasUnderflow(N node) {
    if(node.isLeaf()) {
      return node.getNumEntries() < leafMinimum;
    }
    else {
      return node.getNumEntries() < dirMinimum;
    }
  }

  /**
   * Computes the height of this RTree. Is called by the constructor. and should
   * be overwritten by subclasses if necessary.
   * 
   * @return the height of this RTree
   */
  @Override
  protected int computeHeight() {
    N node = getRoot();
    int height = 1;
  //if (node == null) return height; //added by CZ
    // compute height
    while(!node.isLeaf() && node.getNumEntries() != 0) {
      E entry = node.getEntry(0);
      node = getNode(entry.getID());
      height++;
    }
    return height;
  }

  @Override
  protected void createEmptyRoot(@SuppressWarnings("unused") O object) {
    N root = createNewLeafNode(leafCapacity);
    file.writePage(root);
    setHeight(1);
  }

  /**
   * Performs a bulk load on this RTree with the specified data. Is called by
   * the constructor and should be overwritten by subclasses if necessary.
   * 
   * @param objects the data objects to be indexed
   */
  @Override
  protected void bulkLoad(List<O> objects) {
    StringBuffer msg = new StringBuffer();
    List<SpatialObject> spatialObjects = new ArrayList<SpatialObject>(objects);

    // root is leaf node
    double size = objects.size();
    if(size / (leafCapacity - 1.0) <= 1) {
      N root = createNewLeafNode(leafCapacity);
      root.setID(getRootEntry().getID());
      file.writePage(root);
      createRoot(root, spatialObjects);
      setHeight(1);
      if(logger.isDebugging()) {
        msg.append("\n  numNodes = 1");
      }
    }

    // root is directory node
    else {
      N root = createNewDirectoryNode(dirCapacity);
      root.setID(getRootEntry().getID());
      file.writePage(root);

      // create leaf nodes
      List<N> nodes = createLeafNodes(objects);

      int numNodes = nodes.size();
      System.out.println("leafNodes: " + numNodes);
      if(logger.isDebugging()) {
        msg.append("\n  numLeafNodes = ").append(numNodes);
      }
      setHeight(1);

      // create directory nodes
      while(nodes.size() > (dirCapacity - 1)) {
        nodes = createDirectoryNodes(nodes);
        System.out.println( "created dirNodes: " + nodes.size() );
        numNodes += nodes.size();
        setHeight(getHeight() + 1);
      }

      for( N n : nodes ) System.out.println( n.toString() + " " + n.mbr().toString() );

      // create root
      System.out.println( "nodes in root: " + nodes.size() );
      createRoot(root, new ArrayList<SpatialObject>(nodes));
      numNodes++;
      setHeight(getHeight() + 1);
      if(logger.isDebugging()) {
        msg.append("\n  numNodes = ").append(numNodes);
      }
      System.out.println( "total nodes: " + numNodes );
    }
    if(logger.isDebugging()) {
      msg.append("\n  height = ").append(getHeight());
      msg.append("\n  root " + getRoot());
      logger.debugFine(msg.toString() + "\n");
    }
    System.out.println("total height: " + getHeight());
    System.out.println("dir Capacity: " + dirCapacity);
    System.out.println("leaf Capacity: " + leafCapacity);
  }

  /**
   * Creates and returns the directory nodes for bulk load.
   * 
   * @param nodes the nodes to be inserted
   * @return the directory nodes containing the nodes
   */
  private List<N> createDirectoryNodes(List<N> nodes) {
    int minEntries = dirMinimum;
    int maxEntries = dirCapacity - 1;

    ArrayList<N> result = new ArrayList<N>();
    BulkSplit<N> split = new BulkSplit<N>();
    List<List<N>> partitions = split.partition(nodes, minEntries, maxEntries, bulkLoadStrategy);

    for(List<N> partition : partitions) {
      // create node
      N dirNode = createNewDirectoryNode(dirCapacity);
      file.writePage(dirNode);
      result.add(dirNode);

      // insert nodes
      for(N o : partition) {
        dirNode.addDirectoryEntry(createNewDirectoryEntry(o));
      }

      // write to file
      file.writePage(dirNode);
      if(logger.isDebuggingFiner()) {
        StringBuffer msg = new StringBuffer();
        msg.append("\npageNo ").append(dirNode.getID());
        logger.debugFiner(msg.toString() + "\n");
      }
    }

    return result;
  }

  /**
   * Returns a root node for bulk load. If the objects are data objects a leaf
   * node will be returned, if the objects are nodes a directory node will be
   * returned.
   * 
   * @param root the new root node
   * @param objects the spatial objects to be inserted
   * @return the root node
   */
  @SuppressWarnings("unchecked")
  private N createRoot(N root, List<SpatialObject> objects) {
    // insert data
    for(SpatialObject object : objects) {
      if(object instanceof NumberVector) {
        root.addLeafEntry(createNewLeafEntry((O) object));
      }
      else {
        root.addDirectoryEntry(createNewDirectoryEntry((N) object));
      }
    }

    // set root mbr
    getRootEntry().setMBR(root.mbr());

    // write to file
    //file.writePage(root);
    int returnId = file.writePage(root);
    if(logger.isDebuggingFiner()) {
      StringBuffer msg = new StringBuffer();
      msg.append("pageNo ").append(root.getID());
      logger.debugFiner(msg.toString() + "\n");
    }

    return root;
  }
  
  //==========================================================================
  //The following code is added to support disk based bulk loading. //CZ

  /**
   * Disk based bulk load. The input objects are stored in a file. This
   * method reads record from the file and create a RStarTree.
   */
  public void bulkLoad(O sampleObject, String filename, int size,
  boolean sortLeafFile, int dim) throws Exception 
  {
    initialize(sampleObject);
    int createdNodes = 0;
    int dimension = dim;

    // first create an empty root node for the RStarTree
    N root = createNewDirectoryNode(dirCapacity);
    root.setID(getRootEntry().getID());   // set 0 as the root page ID
    int rootID = file.writePage(root);
 
    int numNodes = 0;  // counter for the number of nodes for debuging use
    //int level = computeLevel(size);
    int pass = 0;
    
    // Create LeafNode
    String prefix = filename;
    String suffix = ".sort";
    String leafFile = null;
    int block = 256;         // external sort buffer size
	//System.out.println("block is : " + block);

    if (sortLeafFile) {
      ExternalSort.externalSort(filename, prefix + suffix, block); 
      System.gc();
      leafFile = prefix + suffix;
    } else
      leafFile = filename; 
 
    ParseLeafFile plf = new ParseLeafFile (leafFile, prefix + pass, dimension, pass, file, sampleObject);

    while(plf.hasNextNode()) {
      plf.getNextNode();
      if (plf.endOfFile)
        break;
      numNodes++;
      //Debug information
/*
      if (numNodes % 3000 == 0)
          System.out.println("pass : " + pass + " , total leaf nodes : " 
                  + numNodes);
*/
    }

	//Delete input file for ParseLeafFile
    File tfile = null;
   	if (sortLeafFile) {	
      tfile = new File(prefix + suffix);
      tfile.delete();
	}
    //System.out.println( "leafNodes: " + numNodes );
    setHeight(1);

    // Repeatly create dirNodes 
    boolean cont = true;
    int nodeCount = numNodes;
    while(cont) {

      setHeight(getHeight() + 1);
      String infile = prefix + pass;   
      ExternalSort.externalSort(infile, prefix + pass + suffix, block); 
      System.gc();
      tfile = new File( infile );
      tfile.delete();

      pass++;

      ParseDirFile pdf = new ParseDirFile(prefix + (pass-1) + suffix, prefix+pass, dimension, pass, root, file);
	  // System.out.println("pass " + pass + "node " + nodeCount);

      // root node case
      if ( nodeCount < dirCapacity ) {
        pdf.handleRoot = true;
        cont = false;
		// System.out.println("root node case");
      }
 
      nodeCount = 0;
      while(pdf.hasNextNode()) {
        pdf.getNextNode();
		if (pdf.endOfFile)
			break;
        nodeCount++;
        numNodes++;
      }
      // System.out.println( "pass : " + pass + " , created dirNodes: " + nodeCount );

      tfile = new File( prefix + (pass-1) + suffix );
      tfile.delete();

    } // while
    // Delete the final file
    tfile = new File(prefix + pass);
    tfile.delete();

    // System.out.println("numNodes: " + numNodes );
    // System.out.println("Height of the tree is : " + getHeight());
    getRoot().integrityCheck();

  } // diskBasedBL

  class ParseDirFile {
    BufferedReader br;
    BufferedWriter bw;
    int dimension;
    boolean flag;
	boolean endOfFile;
    PageFile<N> file;
    public boolean handleRoot;
    N root;
 
    // level
    ParseDirFile(String sortedFile, String outFile, int dim, int pass, N root, 
      PageFile<N> infile) throws Exception {
        
      int bufferSize = 1024 * 1024;    
      this.br = new BufferedReader(new FileReader(sortedFile), bufferSize);  
      if(!handleRoot) 
        this.bw = 
            new BufferedWriter(new FileWriter(outFile), bufferSize);  
      // need to close it?!
      this.dimension = dim;
      this.flag = true;
	  this.endOfFile = false;
      this.file = infile;  // reference to index file
      this.root = root;

    }
 
    boolean hasNextNode() {
      return flag;  
    }
 
    public void getNextNode() throws Exception {
      if (!hasNextNode()) return;
 
      // the number of records required for creating a directory node
      //int num = dirCapacity - 1;
      int num = dirCapacity - 1;
      int counter = 0;
 
      // the list holds the leaf entries used for creating a dirNode
      ArrayList<N> list = new ArrayList<N>(num);
 
      // For directory node, input format <Zorder, NodePageID>
      while(counter < num) {

        String line = br.readLine();
        if (line == null) { 
          flag = false;
          br.close();
          break;
        }

        counter++;
 
        String[] parts = line.split(" ");
        int nodeID = Integer.valueOf(parts[1]); // only need NodePageID here
        N o = file.readPage(nodeID);
        list.add(o);

      }

	  if (counter == 0) {
        	bw.close();
			endOfFile = true;
		  return;
	  }

      N dirNode = null;
      int dirNodeID = -1;
 
      if (handleRoot) {
        // use root dir node
        dirNode = root;
      } else {  
        // create new dir node  
        dirNode = createNewDirectoryNode(dirCapacity);
        dirNodeID = file.writePage(dirNode);
      }

      /*  
      System.out.println("list size: " + list.size()); 
      System.out.println("dirNode size: " + dirNode.getCapacity() );
      System.out.println("dirNode entries: " + dirNode.getNumEntries() );
      System.out.println("dirNode id: " + dirNodeID );
      */

      // insert nodes  
      for (N o : list) {
        if(handleRoot) System.out.println(o.toString() + " " + o.mbr().toString() );
        dirNode.addDirectoryEntry(createNewDirectoryEntry(o));  
      }
 
      // write to file
      file.writePage(dirNode);
 
      if( handleRoot ) return;
 
      // write dirNode information into a disk file
      // output format 
      // <zorder of lower left corner of the leafnode mbr, pageId>
      double[] lf = dirNode.mbr().getMin();
      int [] lfInt = new int[lf.length];
      for (int i = 0; i < lf.length; i++)
        lfInt[i] = (int) lf[i];
 
      String zorder = Zorder.valueOf(lf.length, lfInt);
      String line =  zorder + " " + dirNodeID;
 
      bw.write(line + "\n");  
 
      if (!flag)
        bw.close();
    } // getNextNode

  } // ParseDirFile

  class ParseLeafFile 
  {
    O sampleObject;
    BufferedReader br;
    BufferedWriter bw;
    int dimension;
    boolean flag;
	boolean endOfFile;
    PageFile<N> file;
 
    // level current level of the RStarTree
    ParseLeafFile(String sortedFile, String outFile, int dim, int pass, PageFile<N> infile, O sampleObject) throws Exception 
    {
      int bufferSize = 1024 * 4;
      this.br = new BufferedReader(new FileReader(sortedFile), bufferSize);  
      this.bw = new BufferedWriter(new FileWriter(outFile), bufferSize); 
      this.dimension = dim;
      this.flag = true;
      this.endOfFile = false;
      this.file = infile;  // reference to index file
      this.sampleObject = sampleObject;
    }
 
    boolean hasNextNode() {
      return flag;  
    }
 
    public void getNextNode() throws Exception {
      if (!hasNextNode()) return;
 
      // the number of records required for creating a leaf node
      int num = leafCapacity - 1;
      int counter = 0;
 
      // the list holds the leaf entries used for creating a leafNode
      ArrayList<E> list = new ArrayList<E>(num);
 
      // For leaf node, input format <Zorder, RecordID>
      while(counter < num) {

        String line = br.readLine();
        if (line == null) { 
          flag = false;
          br.close();
          break;
        }
        counter++;
 
        // parse line
        String[] parts = line.split(" ");
        int[] zvalue = Zorder.toCoord(parts[0], dimension);
        double[] fzvalue = new double[dimension];
        for (int i = 0; i < dimension; i++) {
          fzvalue[i] = zvalue[i] * 1d;  
        }

        // create an object and save it into a list
        O o = null;  // need a way to make it generic

        // initialize generic object from double type
        // Since zvalue are all integers, the type of fzvalue doesnot
        // really matter in this case as long as it can fit the entire integer.
        o = sampleObject.newInstance(fzvalue);  
        o.setID(Integer.valueOf(parts[1]));

        list.add(createNewLeafEntry(o));
      }
      
      if (counter == 0) {
         bw.close();           // Leaf node case;
         this.endOfFile = true;
         return;
      }
    
      // create leaf node  
      N leafNode = createNewLeafNode(leafCapacity);
      int leafNodeID = file.writePage(leafNode);
      //System.out.println("leafNode size: " + leafNode.getCapacity() );
    
      // insert data  
      for (E e : list) {
        leafNode.addLeafEntry(e);  
      }
 
      // write to file
      file.writePage(leafNode);
 
      // write leafNode information into a disk file
      // output format 
      // <pageID, zorder, lower left corner of the leafnode mbr >
      double[] lf = leafNode.mbr().getMin();
      int [] lfInt = new int[lf.length];
      for (int i = 0; i < lf.length; i++)
        lfInt[i] = (int) lf[i];
 
      String zorder = Zorder.valueOf(lf.length, lfInt);
      String line = zorder + " " + leafNodeID;
 
      bw.write(line + "\n");  
      if (!flag)
        bw.close();
    } // getNextNode
  } // ParseLeafFile
//=============================================================================

}
