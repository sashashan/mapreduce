package test;
import java.util.Comparator;
class KnnRecordComparator implements Comparator<KnnRecord>{
	public int compare(KnnRecord o1, KnnRecord o2) {
		int ret = 0;
		float dist = o1.getDist() - o2.getDist();

		if (Math.abs((double) dist) < 1E-6) {
			ret = o1.getRid() - o2.getRid();
			//ret = 0;
		} else if (dist > 0)
			ret = 1;
		else if (dist < 0)
			ret = -1;

		return ret;
	}
}
