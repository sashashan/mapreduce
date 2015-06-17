public class Quadtree
{
	private int MAX_OBJECTS = 10; // number of objects a node can hold before it splits
	private int MAX_LEVELS = 5; // the deepest level subnode

	private int level; // the current level
	private List objects;
	private Rectangle bounds; // 2D space that the node occupies 
	// -> Rectangles = what a node is holding. Should change to Zorder in the future 
	private Quadtree[] nodes; // the 4 subnodes

	/*
	 * Constructor
	 */
	public Quadtree (int pLevel, Rectangle pBounds)
	{
		level = pLevel;
		objects = new ArrayList();
		bounds = pBounds;
		nodes = new Quadtree[4]; 
	}

	/*
	 * Clearing the quadtree by recursively clearing all objects from all nodes.
	 */
	public void clear()
	{
		objects.clear(); // removes all the elements from the list

		for (int i = 0; i < nodes.length; i++)
		{
			if (nodes[i] != null) // if there is i'th child, clear it
			{
				nodes[i].clear();
				nodes[i] = null;
			}

		}
	}

	/*
	 * Splits the node into 4 subnodes
	 */
	private void split()
	{
		int subWidth = (int)(bounds.getWidth() / 2);
		int subHeight = (int)(bounds.getHeight() / 2);
		int x = (int)bounds.getX();
		 
		nodes[0] = new Quadtree(level+1, new Rectangle(x + subWidth, y, subWidth, subHeight));
		nodes[1] = new Quadtree(level+1, new Rectangle(x, y, subWidth, subHeight));
		nodes[2] = new Quadtree(level+1, new Rectangle(x, y + subHeight, subWidth, subHeight));
		nodes[3] = new Quadtree(level+1, new Rectangle(x + subWidth, y + subHeight, subWidth, subHeight));
 	}
	
	/*
	 * Determine which node the object belongs to/can fit in.
	 * -1 means object cannot completely it within a child node
	 * and is part of the parent node.
	 */
	public int getIndex(Rectabgle pRect)
	{
		int index = -1;
		double verticalMidpoint = bounds.getX() + (bounds.getWidth() / 2);
		double horizontalMidpoint = bounds.getY() + (bounds.getHeight() / 2);

		// Object can completely fit within the top quadrants
   		boolean topQuadrant = (pRect.getY() < horizontalMidpoint && pRect.getY() + pRect.getHeight() < horizontalMidpoint);
   		// Object can completely fit within the bottom quadrants
   		boolean bottomQuadrant = (pRect.getY() > horizontalMidpoint);

   		// Object can completely fit within the left quadrants
   		if (pRect.getX() < verticalMidpoint && pRect.getX() + pRect.getWidth() < verticalMidpoint) 
   		{
      		if (topQuadrant)
        		index = 1;
      		else if (bottomQuadrant)
        		index = 2;
    	}
   		// Object can completely fit within the right quadrants
    	else if (pRect.getX() > verticalMidpoint)
    		if (topQuadrant)
      			index = 0;
     		else if (bottomQuadrant)
       			index = 3;
   		return index;
	}

	/*
	 * Inserts an object into a quadtree. 
	 * If the node exceeds the capacity, it will split and add all the objects
	 * to their corresponding nodes.
	 */
	public void insert(Rectangle pRect)
	{
		if (nodes[0] != null) // has the node been split yet?
		{
    		int index = getIndex(pRect);
 
    		if (index != -1) 
    		{
    			nodes[index].insert(pRect);
	    		return;
    		}
    	}

    	objects.add(pRect);
 
		if (objects.size() > MAX_OBJECTS && level < MAX_LEVELS) 
		{
    		if (nodes[0] == null)
        		split(); 
 
    		int i = 0;
     		while (i < objects.size()) 
     		{
       			int index = getIndex(objects.get(i));
       			if (index != -1)
         			nodes[index].insert(objects.remove(i));
       			else
         			i++;
     		}
   		}
   	}

   	/*
 	 * Return all objects that could collide with the given object
 	 */
 	public List retrieve(List returnObjects, Rectangle pRect) 
 	{
  		int index = getIndex(pRect);
   		if (index != -1 && nodes[0] != null)
    		nodes[index].retrieve(returnObjects, pRect);
 
   		returnObjects.addAll(objects);
 
   		return returnObjects;
 	}

} // end class