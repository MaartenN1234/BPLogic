package mn.bplogic.rowsources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import mn.bplogic.main.MemoryStatics;
import mn.bplogic.main.MemoryStaticsReporter;
import mn.bplogic.main.MemoryStaticsReporterMessage;
import mn.bplogic.rowsources.exceptions.DataModelException;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;


public class BPTable extends BPRowSource implements MemoryStaticsReporter{
	private int                 rowCount;
	private int                 sortCount;
	private boolean             hasHadFirstTrueMaterialize;
	private ArrayList<BPRow[]>  dataSortStore;
	private ArrayList<String[]> sortColumnsStore;
	private ArrayList<int[]>    sortIndexesStore;
	private long                totalSortTime;

	BPTable(String tableName, int[] intTypes, String[] intFieldLabels, String[] doubleFieldLabels, BPRow[] data) {
		this(tableName, intTypes, intFieldLabels, doubleFieldLabels, data, null, false);
	}
	BPTable(String tableName, int[] intTypes, String[] intFieldLabels, String[] doubleFieldLabels, BPRow[] data, boolean applyGlueAndPaste) {
		this(tableName, intTypes, intFieldLabels, doubleFieldLabels, data, null, applyGlueAndPaste);
	}
	BPTable(String tableName, int[] intTypes, String[] intFieldLabels, String[] doubleFieldLabels, BPRow[] data, String[] sortLabels) {
		this(tableName, intTypes, intFieldLabels, doubleFieldLabels, data, sortLabels, false);
	}

	BPTable(String tableName, int[] intTypes, String[] intFieldLabels, String[] doubleFieldLabels, BPRow[] data, String[] sortLabels, boolean applyGlueAndPaste) {
		this.rowSourceName     = tableName;
		this.dataSortStore     = new ArrayList<BPRow[]>();
		this.intFieldLabels    = intFieldLabels;
		this.doubleFieldLabels = doubleFieldLabels;
		this.intTypes          = intTypes;
		this.totalSortTime     = 0;
		this.sortCount         = 0;

		this.sortColumnsStore  = new ArrayList<String[]>();
		this.sortIndexesStore  = new ArrayList<int[]>();
		BPRow[] dataUse = data;
		if(applyGlueAndPaste){
			dataUse = BPRow.applyGlueAndPaste(dataUse, BPRow.GLUE_AND_PASTE_SORT_SINGLE_FILTER, false);
			String [] fullSortLabels = getAllFieldLabels();
			this.sortIndexesStore.add(getSortIndexes(fullSortLabels));
			this.sortColumnsStore.add(fullSortLabels);
			this.hasHadFirstTrueMaterialize = true;
		} else {
			try {
				this.sortIndexesStore.add(getSortIndexes(sortLabels));
				this.sortColumnsStore.add(sortLabels);
				this.hasHadFirstTrueMaterialize = (sortLabels != null);
			} catch (UnknownColumnException e) {
				this.sortIndexesStore.add(null);
				this.sortColumnsStore.add(null);
				this.hasHadFirstTrueMaterialize = false;
			}
		}
		this.dataSortStore.add(dataUse);
		this.rowCount  = dataUse.length;
		MemoryStatics.register(this);
	}

	public int getRowCount(){
		return rowCount;
	}
	public int deepRowCount(){
		return rowCount;
	}
	public String[] getMinimalEffortSortings() {
		return sortColumnsStore.toArray(new String[0]);
	}


	// private method to find (or create) applicant sortSet
	private int getPartitionSortSet(int effortLevel, String [] partitionColumns, String [] subSortColumns)  throws DataModelException{
		// Shortcuts
		if (rowCount ==0) return 0;
		if (partitionColumns == null && subSortColumns == null) return 0;

		// Determine sortset to be used
		int partColumnsCnt  = (partitionColumns == null) ? 0 : partitionColumns.length;
		int subSColumnsCnt  = (subSortColumns   == null) ? 0 : subSortColumns.length;
		int useSetOption = 0;
		for (String [] ss: sortColumnsStore){
			if (ss != null && ss.length >= partColumnsCnt + subSColumnsCnt){
				// test Sorting
				boolean applies = true;
				if (subSortColumns != null){
					if (subSColumnsCnt + partColumnsCnt != ss.length){
						applies = false;
					} else {
						for (int i = partColumnsCnt; i< ss.length; i++){
							if (!ss[i].equals(subSortColumns[i])){
								applies = false;
							}
						}
					}
				}

				// test Partitioning
				for (int i=0; applies && i<partColumnsCnt; i++){
					boolean found = false;
					for (int j=0; j<partColumnsCnt; j++)
						found = found || ss[j].equals(partitionColumns[i]);
					applies = applies && found;
				}

				if (applies){
					return useSetOption;
				}
			}
			useSetOption++;
		}

		if (effortLevel == MINIMAL_EFFORT)
			return -1;
		if (effortLevel == LIMITED_EFFORT && hasHadFirstTrueMaterialize)
			return -1;

		ArrayList<String> buffer = new ArrayList<String>();
		if(partitionColumns != null)
			buffer.addAll(Arrays.asList(partitionColumns));
		if(subSortColumns != null)
			buffer.addAll(Arrays.asList(subSortColumns));
		sort(buffer.toArray(new String[0]));
		return sortColumnsStore.size()-1;
	}





	private void sort(String [] sortColumns) throws DataModelException{
		long startSortTimeStamp = System.currentTimeMillis();

		int [] sortIndexes = getSortIndexes(sortColumns);
		Comparator<BPRow> comparator = new BPRow.ColumnListSortComparator(sortIndexes);

		BPRow []  bpr;
		if(hasHadFirstTrueMaterialize){
			bpr = new BPRow[rowCount];
			// maybe improve (find closest sorted set)
			System.arraycopy(dataSortStore.get(0), 0, bpr, 0, rowCount);
			dataSortStore.add(bpr);
			sortColumnsStore.add(sortColumns);
			sortIndexesStore.add(sortIndexes);
		} else {
			bpr = dataSortStore.get(0);
			sortColumnsStore.set(0, sortColumns);
			sortIndexesStore.set(0, sortIndexes);
			hasHadFirstTrueMaterialize = true;
		}
		Arrays.sort(bpr, comparator);


		long stopSortTimeStamp = System.currentTimeMillis();
		totalSortTime += (stopSortTimeStamp - startSortTimeStamp);
		sortCount ++;
	}
	private int [] getSortIndexes(String [] sortLabels) throws UnknownColumnException{
		if (sortLabels == null)
			return null;

		int [] sortIndexes = new int[sortLabels.length];

		HashMap<String, Integer> labelMapping = new HashMap<String, Integer>();
		int i = 1;
		if(intFieldLabels != null)
			for (String s : intFieldLabels)
				labelMapping.put(s, i++);

		i = -1;
		if(doubleFieldLabels != null)
			for (String s : doubleFieldLabels)
				labelMapping.put(s, i--);

		i = 0;
		for (String s: sortLabels) {
			Integer j = labelMapping.get(s);
			if (j == null){
				throw new UnknownColumnException(s, rowSourceName);
			}
			sortIndexes[i++] = j;
		}
		return sortIndexes;
	}



	public String getTechnicalFootprint() {
		StringBuffer sortReps = new StringBuffer();
		boolean isEmpty       = true;
		for (String [] s : sortColumnsStore){
			if (!isEmpty) sortReps.append(", ");
			sortReps.append(Arrays.toString(s));
			isEmpty = false;
		}
		return "BPTable "+ rowSourceName + "("+hashCode()+")\t\tRows: "+ rowCount +"\tSorttime: "+ totalSortTime+"ms"+"\tSorts: "+ sortCount+" ("+sortReps+")";
	}


	public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValuesIn, String[] subSortColumns){
		// Shortcut empty set
		if (rowCount == 0) return new InternalIterator(0, 0, 0);

		// GetSorting
		int useSortSet = getPartitionSortSet(effort, predicateColumns, subSortColumns);
		if (useSortSet < 0) return null;
		if (predicateValuesIn == null) return new InternalIterator(0, rowCount, useSortSet);

		// Apply predicates
		// Check 1
		if (predicateValuesIn.length != predicateColumns.length){
			throw new IllegalArgumentException("Number of predicate columns and predicate values don't match.");
		}
		// Check 2
		for (int i=0; i<predicateColumns.length; i++){
			if (sortIndexesStore.get(useSortSet)[i] < 0) {
				throw new UnsupportedOperationException("Predicate is on doubles which can't be used for predicates");
			}
		}

		// ensure matching order of columns
		int [] predicateValues = new int[predicateValuesIn.length];
		int i=0;

		for (String s : sortColumnsStore.get(useSortSet)){
			int j;
			for (j=0; !s.equals(predicateColumns[j]); j++);
			predicateValues[i++] = predicateValuesIn[j];
		}


		// find offsets
		int stepSize = 1;
		int limit    = (1+rowCount)/4;
		while (stepSize <= limit){
			stepSize *=2;
		}

		int minIndex = stepSize*2-1;
		int maxIndex = minIndex;

		while (stepSize > 0){
			int compMin = comparePredicateValues(predicateValues, useSortSet, minIndex);
			int compMax = (minIndex == maxIndex) ? compMin :  comparePredicateValues(predicateValues, useSortSet, maxIndex);

			if (compMin == 1){
				minIndex += stepSize;
			} else {
				minIndex -= stepSize;
			}
			if (compMax == -1){
				maxIndex -= stepSize;
			} else {
				maxIndex += stepSize;
			}
			stepSize = stepSize / 2;
		}
		if (minIndex != rowCount){
			int compMin = comparePredicateValues(predicateValues, useSortSet, minIndex);
			int compMax = (minIndex == maxIndex) ? compMin :  comparePredicateValues(predicateValues, useSortSet, maxIndex);
			minIndex += compMin;
			maxIndex += compMax;
		}

		// result
		return new InternalIterator(minIndex, maxIndex+1, useSortSet);
	}
	private int comparePredicateValues(int [] predicateValues, int useSet, int index){
		if (index >= rowCount)
			return -1;

		BPRow compBPRow   = dataSortStore.get(useSet)[index];
		int[] sortIndexes = sortIndexesStore.get(useSet);
		for (int i=0; i< predicateValues.length; i++){
			int pointData = compBPRow.intValues[sortIndexes[i]-1];
			if (predicateValues[i] < pointData) {
				return -1;
			}else if (predicateValues[i] > pointData){
				return 1;
			}
		}
		return 0;
	}
	public void finalize (){
		MemoryStatics.register(new MemoryStaticsReporterMessage("RECLAIMED: "+ this.getTechnicalFootprint()));
	}

	// Iterator implementation
	private class InternalIterator extends BPRowIterator{
		int     location;
		int     size;
		BPRow[] data;
		String [] sorting;
		private InternalIterator (int minIndex, int beyondIndex, int sortedSet){
			hasHadFirstTrueMaterialize = true;
			this.location = minIndex > 0 ? minIndex : 0;
			this.size     = rowCount < beyondIndex ? rowCount : beyondIndex;
			this.data     = dataSortStore.get(sortedSet);
			this.sorting  = sortColumnsStore.get(sortedSet);
		}

		protected BPRow[] internalFetch(int suggestedSize) {
			return nextBuffer(suggestedSize);
		}

		// direct (unbuffered) overrules
		public BPRow [] nextBuffer(int bufferSize){
			int remainderSize = size-location;
			int fetchSize     = remainderSize > bufferSize ? bufferSize : remainderSize;
			if (fetchSize <= 0){
				return new BPRow[0];
			}

			BPRow [] output =  new BPRow[fetchSize];
			System.arraycopy(data, location, output, 0, fetchSize);
			location = location + fetchSize;
			return output;
		}
		public String[] getSorting() {
			return sorting;
		}
	}

}
