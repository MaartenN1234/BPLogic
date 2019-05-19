package mn.bplogic.rowsources;

import java.sql.Date;
import java.util.Arrays;

import mn.bplogic.main.Settings;
import mn.bplogic.main.TranslateMap;


public abstract class BPRowSource {
	final static int specialSD  = -9;
	final static int specialED  = -8;
	final static int specialIgnore  = -1;
	public final static int intType    = 0;
	public final static int stringType = 1;
	public final static int dateType   = 2;
	public final static int objectType = 3;
	public final static int doubleType = 4;

	final static long halfDayMillis = 12*3600*1000;
	final static long oneDayMillis  = 24*3600*1000;

	protected String[]  intFieldLabels;
	protected String[]  doubleFieldLabels;
	protected int[]     intTypes;
	protected String    rowSourceName;
	protected int       rowCount = -1;
	private String[]    allFieldLabels;

	// Row count for the underlying in-memory collection
	public abstract int deepRowCount();

	public final static int FORCE_RESULT   = 0;
	public final static int LIMITED_EFFORT = 1;
	public final static int MINIMAL_EFFORT = 2;

	public BPRowIterator getIterator(){
		return getPartitionedIterator(FORCE_RESULT, null, null);
	}
	public BPRowIterator getSortedIterator(String[] sortColumns) {
		return getSortedIterator(FORCE_RESULT, sortColumns);
	}
	public BPRowIterator getPartitionedIterator(String[] partitionColumns){
		return getPartitionedIterator(FORCE_RESULT,partitionColumns);
	}
	public BPRowIterator getPartitionedIterator (String [] partitionColumns, String[] subSortColumns){
		return getPartitionedIterator(FORCE_RESULT, partitionColumns, subSortColumns);
	}
	public BPRowIterator getPredicatedIterator(String[] predicateColumns, int[] predicateValues){
		return getPredicatedIterator(FORCE_RESULT, predicateColumns, predicateValues);
	}
	public BPRowIterator getSortedIterator(int effort, String[] sortColumns) {
		return getPartitionedIterator(effort, null, sortColumns);
	}
	public BPRowIterator getPartitionedIterator(int effort, String[] partitionColumns){
		return getPredicatedIterator(effort, partitionColumns, null);
	}
	public BPRowIterator getPartitionedIterator (int effort, String [] partitionColumns, String[] subSortColumns){
		return getPredicatedIterator(effort, partitionColumns, null, subSortColumns);
	}

	public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValues){
		return getPredicatedIterator(effort, predicateColumns, predicateValues, null);
	}

	public abstract BPRowIterator getPredicatedIterator  (int effort, String [] predicateColumns, int [] predicateValues, String[] subSortColumns);

	public abstract String[] getMinimalEffortSortings();

	// Implementer classes are requested to implement this method if rowcount can be established in
	// another way than using the iterator
	protected int getRowCountFast(){
		return rowCount;
	}
	public final int getRowCount(boolean forceUseIterator){
		if (rowCount != -1)
			return rowCount;

		rowCount = getRowCountFast();
		if (rowCount == -1 && forceUseIterator){
			final int stepSize = Settings.InternalFetchSize;
			BPRowIterator bpIter = getIterator();
			rowCount = 0;
			BPRow [] bpr = bpIter.nextBuffer(stepSize);
			while (bpr.length > 0){
				rowCount += bpr.length;
				bpr = bpIter.nextBuffer(stepSize);
			}
		}
		return rowCount;
	}



	public String toString(){
		final int showRowCount = Settings.ShowRowCount;

		TranslateMap<String> tms = TranslateMap.getStringMap();
		TranslateMap<Date>   tmd = TranslateMap.getDateMap();
		TranslateMap<Object> tmo = TranslateMap.getObjectMap();

		StringBuffer sb = new StringBuffer();

		// Header
		sb.append("STARTDATE\tENDDATE \t");
		for (int i=0; i<intFieldLabels.length; i++)
			sb.append(intFieldLabels[i]+"\t");
		for (int i=0; i<doubleFieldLabels.length; i++)
			sb.append(doubleFieldLabels[i]+"\t");
		sb.append("\n");
		sb.append("\n");

		// Data
		BPRowIterator bpIter = this.getIterator();
		BPRow [] bprBuf = bpIter.nextBuffer(showRowCount);
		for (int counter=0; counter < bprBuf.length; counter++){
			BPRow bpr = bprBuf[counter];
			if (bpr != null){
				sb.append(new Date((bpr.startIncl)*oneDayMillis)+"\t");
				sb.append(new Date((bpr.end-1)*oneDayMillis)+"\t");
				for (int i=0; i<intFieldLabels.length; i++){
					int k = bpr.intValues[i];
					switch (intTypes[i]){
					case intType:
						if (k==BPRow.NULL_INT)
							sb.append("null\t");
						else
							sb.append(k+"\t");
						break;
					case stringType:
						sb.append(tms.translate(k)+"\t");;
						break;
					case dateType:
						sb.append(tmd.translate(k)+"\t");;
						break;
					case objectType:
						sb.append(tmo.translate(k)+"\t");;
						break;
					}
				}
				for (int i=0; i<doubleFieldLabels.length; i++){
					double d = bpr.doubleValues[i];
					if (d==BPRow.NULL_DOUBLE)
						sb.append("null\t");
					else
						sb.append(d+"\t");
				}
			} else {
				sb.append("<NULL>");
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	public final String[] getIntFieldLabels() {
		return intFieldLabels;
	}

	public final String[] getDoubleFieldLabels() {
		return doubleFieldLabels;
	}
	public final String[] getAllFieldLabels() {
		if (allFieldLabels == null){
			allFieldLabels = Arrays.copyOf(intFieldLabels, intFieldLabels.length+doubleFieldLabels.length);
			System.arraycopy(doubleFieldLabels, 0, allFieldLabels, intFieldLabels.length, doubleFieldLabels.length);
		}
		return allFieldLabels;
	}
	public final int[] getIntTypes() {
		return intTypes;
	}

}
