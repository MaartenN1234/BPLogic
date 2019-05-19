package mn.bplogic.rowsources;
import java.sql.Date;
import java.util.Arrays;
import java.util.Comparator;


public class BPRow implements Cloneable {
	final public static int       NULL_INT          = Integer.MIN_VALUE;
	final public static double    NULL_DOUBLE       = Double.MIN_VALUE;
	final private static int      NULL_ARRAY_LENGTH = 1024;
	final private static int[]    NULL_INT_ARRAY    = new int[NULL_ARRAY_LENGTH];
	final private static double[] NULL_DOUBLE_ARRAY = new double[NULL_ARRAY_LENGTH];
	static{
		Arrays.fill(NULL_INT_ARRAY,    NULL_INT);
		Arrays.fill(NULL_DOUBLE_ARRAY, NULL_DOUBLE);
	}

	public int startIncl;
	public int end;
	public int []    intValues;
	public double [] doubleValues;

	public BPRow(int nrInts, int nrDbls) {
		if (nrInts >= 0){
			intValues    = new int[nrInts];
		}
		if (nrDbls >= 0){
			doubleValues = new double[nrDbls];
		}
	}
	public BPRow(int nrInts, int nrDbls, boolean NULL_INIT) {
		this(nrInts, nrDbls);
		for (int i = 0; i<nrInts; i+=NULL_ARRAY_LENGTH){
			int size = nrInts - i;
			size     = size > NULL_ARRAY_LENGTH ? NULL_ARRAY_LENGTH : size;
			System.arraycopy(NULL_INT_ARRAY,   0, intValues, i, size);
		}
		for (int i = 0; i<nrDbls; i+=NULL_ARRAY_LENGTH){
			int size = nrDbls - i;
			size     = size > NULL_ARRAY_LENGTH ? NULL_ARRAY_LENGTH : size;
			System.arraycopy(NULL_DOUBLE_ARRAY,   0, doubleValues, i, size);
		}


	}

	public BPRow clone(){
		try {
			return (BPRow) (super.clone());
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException (e);
		}
	}
	public BPRow cloneDeep(){
		BPRow output        = clone();
		output.intValues    = new int[intValues.length];
		output.doubleValues = new double[doubleValues.length];
		System.arraycopy(intValues,    0, output.intValues,    0, intValues.length);
		System.arraycopy(doubleValues, 0, output.doubleValues, 0, doubleValues.length);
		return output;
	}
	public String toString(){
		return new Date((startIncl)*BPRowSource.oneDayMillis) +" - " +
				new Date((end-1)*BPRowSource.oneDayMillis) + ":\t" + Arrays.toString(intValues) + ", "+Arrays.toString(doubleValues);

	}
	public void extend(int nrInts, int nrDoubles, boolean atEnd) {
		int []    tempInts    = intValues;
		double [] tempDoubles = doubleValues;
		intValues    = new int[intValues.length + nrInts];
		doubleValues = new double[doubleValues.length + nrDoubles];

		int startPosNullInts    = 0;
		int startPosNullDoubles = 0;
		if (atEnd){
			System.arraycopy(tempInts,    0, intValues,    0, tempInts.length);
			System.arraycopy(tempDoubles, 0, doubleValues, 0, tempDoubles.length);
			startPosNullInts   = intValues.length;
			startPosNullDoubles = doubleValues.length;
		} else {
			System.arraycopy(tempInts,    0, intValues,    nrInts,    tempInts.length);
			System.arraycopy(tempDoubles, 0, doubleValues, nrDoubles, tempDoubles.length);
		}
		for (int i = 0; i<nrInts; i+=NULL_ARRAY_LENGTH){
			int size = nrInts - i;
			size     = size > NULL_ARRAY_LENGTH ? NULL_ARRAY_LENGTH : size;
			System.arraycopy(NULL_INT_ARRAY,   0, intValues,     startPosNullInts+i,    size);
		}
		for (int i = 0; i<nrDoubles; i+=NULL_ARRAY_LENGTH){
			int size = nrDoubles - i;
			size     = size > NULL_ARRAY_LENGTH ? NULL_ARRAY_LENGTH : size;
			System.arraycopy(NULL_DOUBLE_ARRAY, 0, doubleValues, startPosNullDoubles+i, size);
		}
	}


	/* Static utility methods */
	static BPRow join(BPRow left, BPRow right){
		int startIncl = left.startIncl > right.startIncl ? left.startIncl : right.startIncl;
		int end       = left.end       < right.end       ? left.end       : right.end;
		if (end <= startIncl)
			return null;

		BPRow output = new BPRow(left.intValues.length + right.intValues.length,
				left.doubleValues.length + right.doubleValues.length);

		output.startIncl = startIncl;
		output.end       = end;
		System.arraycopy(left.intValues,     0, output.intValues,    0,                        left.intValues.length);
		System.arraycopy(right.intValues,    0, output.intValues,    left.intValues.length,    right.intValues.length);
		System.arraycopy(left.doubleValues,  0, output.doubleValues, 0,                        left.doubleValues.length);
		System.arraycopy(right.doubleValues, 0, output.doubleValues, left.doubleValues.length, right.doubleValues.length);
		return output;
	}

	static int GLUE_AND_PASTE_NOSORT             = 0;
	static int GLUE_AND_PASTE_SORT_SINGLE_FILTER = 1;
	static int GLUE_AND_PASTE_SORT_DUAL_FILTER   = 2;

	static BPRow [] applyGlueAndPaste(BPRow[] input, int useSort, boolean forceCopyInput){
		if (input == null || input.length < 2)
			return input;
		BPRow [] buffer = input;
		if (forceCopyInput){
			buffer = shrinkArray(input, input.length);
		}

		if (useSort == GLUE_AND_PASTE_SORT_DUAL_FILTER){
			buffer = applyGlueAndPasteInternal(buffer);
		}

		if (useSort > GLUE_AND_PASTE_NOSORT){
			Comparator<BPRow> comparator = BPRow.FULL_VALUE_COMPARATOR;
			Arrays.sort(buffer, comparator);
		}

		return applyGlueAndPasteInternal(buffer);
	}
	static BPRow[] shrinkArray(BPRow[] input, int size){
		int sizeOut = size > input.length ? input.length : size;
		BPRow [] output = new BPRow[sizeOut];
		System.arraycopy(input, 0, output, 0, sizeOut);
		return output;
	}
	static BPRow[] applyGlueAndPasteInternal(BPRow[] input){
		int refIndex   = 0;
		int outputSize = 1;
		for(int i = 1; i< input.length; i++){
			if (input[refIndex].end == input[i].startIncl && input[refIndex].equalValuesUnchecked(input[i])){
				input[refIndex].end = input[i].end;
				input[i] = null;
			} else {
				outputSize++;
				refIndex = i;
			}
		}
		if (outputSize == input.length)
			return input;

		BPRow[] output = new BPRow[outputSize];
		int j=0;
		for (BPRow bpr : input)
			if (bpr!= null)
				output[j++] = bpr;
		return output;
	}
	private boolean equalValuesUnchecked(BPRow other){
		for(int i=0; i< intValues.length; i++)
			if (intValues[i]!=other.intValues[i])
				return false;

		for(int i=0; i< doubleValues.length; i++)
			if (doubleValues[i]!=other.doubleValues[i])
				return false;

		return true;
	}

	/* Comparator Classes */
	final public static Comparator<BPRow> NO_VALUE_COMPARATOR   = new SortComparator();
	final public static Comparator<BPRow> FULL_VALUE_COMPARATOR = new FullSortComparator();

	static class SortComparator implements Comparator<BPRow>{
		public int compare(BPRow o1, BPRow o2){
			// technical interval compare
			if (o1.startIncl < o2.startIncl) {
				return -1;
			} else if (o1.startIncl > o2.startIncl) {
				return 1;
			}
			// Enddates other way around for internal technical purposes
			if (o1.end > o2.end) {
				return -1;
			} else if (o1.end < o2.end) {
				return 1;
			}
			// equality
			return 0;
		}
	}

	static class FullSortComparator extends SortComparator{
		public int compare(BPRow o1, BPRow o2) {
			int [] o1i = o1.intValues;
			int [] o2i = o2.intValues;
			for(int i=0; i< o1i.length; i++){
				if (o1i[i] < o2i[i]) {
					return -1;
				} else if (o1i[i] > o2i[i]) {
					return 1;
				}
			}
			double [] o1d = o1.doubleValues;
			if (o1d.length > 0){
				double [] o2d = o2.doubleValues;
				for(int i=0; i< o1d.length; i++){
					if (o1d[i] < o2d[i]) {
						return -1;
					} else if (o1d[i] > o2d[i]) {
						return 1;
					}
				}
			}
			// technical interval compare
			return super.compare(o1, o2);
		}
	}
	static class ColumnListSortComparator extends SortComparator{
		private int[] columnList;
		ColumnListSortComparator(int [] columnList){
			if (columnList == null || columnList.length == 0){
				this.columnList = null;
			} else {
				this.columnList = columnList;
			}
		}
		public int compare(BPRow o1, BPRow o2) {
			// functional requested compare
			if(columnList != null){
				for (int i:columnList){
					if (i>0){
						int o1c =o1.intValues[i-1];
						int o2c =o2.intValues[i-1];
						if (o1c < o2c) {
							return -1;
						} else if (o1c > o2c) {
							return 1;
						}
					}else if (i<0){
						double o1c =o1.doubleValues[-i-1];
						double o2c =o2.doubleValues[-i-1];
						if (o1c < o2c) {
							return -1;
						} else if (o1c > o2c) {
							return 1;
						}
					}
				}
			}
			// technical interval compare
			return super.compare(o1, o2);
		}
	}

}
