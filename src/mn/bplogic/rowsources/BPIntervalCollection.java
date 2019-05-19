package mn.bplogic.rowsources;

import java.util.ArrayList;
import java.util.List;

public class BPIntervalCollection {
	private List<int[]> storage;

	public BPIntervalCollection(BPRow[] source) {
		int lastSD = 0;
		int lastED = Integer.MIN_VALUE;

		storage = new ArrayList<int[]>();

		for (BPRow s : source){
			if (s.startIncl < lastED){
				lastED = lastED > s.end       ? lastED : s.end;
				lastSD = lastSD < s.startIncl ? lastSD : s.startIncl;
			} else {
				if (lastED != Integer.MIN_VALUE){
					storage.add(new int[]{lastSD, lastED});
				}
				lastSD = s.startIncl;
				lastED = s.end;
			}
		}
		if (lastED != Integer.MIN_VALUE){
			storage.add(new int[]{lastSD, lastED});
		}
	}

	public void minus(BPIntervalCollection other) {
		if (other.isEmpty())
			return;

		int[][] me  = storage.toArray(new int[0][0]);
		int[][] min = other.storage.toArray(new int[0][0]);

		int minIx     = 0;
		int lastMinSD = min[0][0];
		int lastMinED = min[0][1];

		storage = new ArrayList<int[]>();
		for (int [] s: me){
			// in case we need advance of min cursor
			while (minIx < min.length && lastMinED < s[0]){
				minIx++;
				lastMinSD = minIx < min.length ? min[minIx][0] : Integer.MAX_VALUE;
				lastMinED = minIx < min.length ? min[minIx][1] : Integer.MAX_VALUE;
			}

			// overlap
			while (minIx < min.length && s[0] < lastMinED && s[1] > lastMinSD){
				if (s[0] < lastMinSD){
					storage.add(new int[]{s[0], lastMinSD});
				}
				if (s[1] > lastMinED){
					s[0] = lastMinED;
				} else {
					break;
				}
				minIx++;
				lastMinSD = minIx < min.length ? min[minIx][0] : Integer.MAX_VALUE;
				lastMinED = minIx < min.length ? min[minIx][1] : Integer.MAX_VALUE;
			}

		}
	}

	public boolean isEmpty() {
		return storage.isEmpty();
	}

	public BPRow[] asOJArray(int nrInts, int nrDoubles) {
		BPRow [] output = new BPRow[storage.size()];
		int i = 0;
		for(int [] is : storage){
			BPRow o     = new BPRow(nrInts, nrDoubles, true);
			o.startIncl = is[0];
			o.end       = is[1];
			output[i++] = o;
		}
		return output;
	}

}
