import java.lang.*;

public class Test
{
	public static void main (String[] args)
	{ 
		List allObjects = new ArrayList();
		StringBuilder sb;
		Random r = new Random();

		Quadtree quadtree = new Quadtree(0, new Rectangle(0, 0, 10, 10));

		System.out.println("S Points");
		for (int i = 0; i < 20; i++)
		{
			int rand = r.nextInt(11);
			int rand2 = r.nextInt(11);
			System.out.println("Point " + i + " : X: " + rand + " Y: " + rand2);
			Rectable rec = new Rectangle (rand, rand2, 1, 1);
			allObjects.add(rec);
		}

		// Objects of concern (potentially k)
		System.out.println("K Points");
		List kObjects = new ArrayList();
		for (int i = 0; i < 3; i++)
		{
			int rand = r.nextInt(11);
			int rand2 = r.nextInt(11);
			System.out.println("Point " + i + " : X: " + rand + " Y: " + rand2);
			Rectable rec = new Rectangle (rand, rand2, 1, 1);
			kObjects.add(rec);
		}

		for (int i = 0; i < allObjects.size(); i++)
		{
			quadtree.insert(allObjects.get(i));
		}

		List returnObject = new ArrayList();

		for (int i = 0; i < allObjects.size(); i++)
		{
			System.out.println("K: " + i);
			returnObject.clear();
			quadtree.retrieve(returnObjects, objects.get(i));
			System.out.println("returnObjects are: ");
			for (int i = 0; i < returnObjects.size(); i ++)
			{
				System.out.println("X: " + returnObjects.get(i).);
			}
		}
	}
}