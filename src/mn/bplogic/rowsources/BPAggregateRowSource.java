package mn.bplogic.rowsources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;



import mn.bplogic.api.AggregateConverter;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;

public class BPAggregateRowSource extends BPBufferRowSource {

	public BPAggregateRowSource(String rowSourceName, BPRowSource source, String [] groupbyKeys, AggregateConverter projFilter) {
		super();
		BPRowSource internalRS = new BPAggregateRowSourceInternal(rowSourceName, source, groupbyKeys, projFilter);
		init(internalRS, BUFFER_ON_FIRST_FETCH);
	}

	class BPAggregateRowSourceInternal extends BPRowSource {
		protected BPRowSource      source;
		protected AggregateConverter projFilter;
		protected HashSet<String>  keyFieldLabels;
		protected int[]            keyFieldPositions;

		BPAggregateRowSourceInternal(String rowSourceName, BPRowSource source, String [] groupbyKeys, AggregateConverter projFilter){
			this.rowSourceName     = rowSourceName;
			this.source            = source;
			this.keyFieldLabels    = new HashSet<String>();
			this.keyFieldPositions = new int[groupbyKeys.length];
			this.keyFieldLabels.addAll(Arrays.asList(groupbyKeys));
			HashMap<String, Integer>  findTypeMap           = new HashMap<String, Integer>();
			HashMap<String, Integer>  findLocMap            = new HashMap<String, Integer>();

			ArrayList<String>         intFieldLabelsTemp    = new ArrayList<String>();
			ArrayList<String>         doubleFieldLabelsTemp = new ArrayList<String>();
			ArrayList<Integer>        intFieldTypesTemp     = new ArrayList<Integer>();
			for (int i=0; i< source.intFieldLabels.length; i++){
				findTypeMap.put(source.intFieldLabels[i], source.intTypes[i]);
				findLocMap.put(source.intFieldLabels[i], i);
			}
			for (int i=0; i< source.doubleFieldLabels.length; i++){
				findLocMap.put(source.doubleFieldLabels[i], -1-i);
			}
			int i = 0;
			for (String s : groupbyKeys){
				Integer k = findLocMap.get(s);
				if (k == null){
					throw new UnknownColumnException(s, source.rowSourceName);
				}
				keyFieldPositions[i++] = k;
			}

			this.projFilter        = projFilter;
			intFieldLabelsTemp.addAll   (Arrays.asList(projFilter.getIntFieldLabels()));
			doubleFieldLabelsTemp.addAll(Arrays.asList(projFilter.getDoubleFieldLabels()));
			this.intFieldLabels    = intFieldLabelsTemp.toArray(new String[0]);
			this.doubleFieldLabels = doubleFieldLabelsTemp.toArray(new String[0]);
			this.intTypes          = new int[intFieldTypesTemp.size() + projFilter.getIntTypes().length];
			i = 0;
			for (int k : intFieldTypesTemp)
				this.intTypes[i++] = k;
			for (int k : projFilter.getIntTypes())
				this.intTypes[i++] = k;
		}

		public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValues, String[] subSortColumns) {
			// Since this may return null in certain cases even if effort = FORCE_RESULT a surrounding buffer is required
			String [] keyFields = keyFieldLabels.toArray(new String[0]);
			boolean canBeDelivered = false;
			if (subSortColumns != null && subSortColumns.length == 0 &&
					((predicateColumns == null && keyFields.length==0) || predicateColumns.equals(keyFields)))
				canBeDelivered = true;
			if (subSortColumns == null && (predicateColumns == null || keyFieldLabels.containsAll(Arrays.asList(predicateColumns))))
				canBeDelivered = true;

			canBeDelivered = canBeDelivered && predicateValues == null;

			if (!canBeDelivered)
				return null;

			BPRowIterator sourceIter;
			sourceIter = source.getPredicatedIterator(LIMITED_EFFORT, keyFieldLabels.toArray(new String[0]), null, new String[0]);
			if (sourceIter != null)
				return new InternalIterator(sourceIter, false);
			sourceIter = source.getPredicatedIterator(MINIMAL_EFFORT, keyFieldLabels.toArray(new String[0]), null, null);
			if (sourceIter != null)
				return new InternalIterator(sourceIter, true);
			sourceIter = source.getPredicatedIterator(effort, keyFieldLabels.toArray(new String[0]), null, new String[0]);
			if (sourceIter != null)
				return new InternalIterator(sourceIter, false);
			return null;
		}

		public int deepRowCount() {
			return source.deepRowCount();
		}
		public String[] getMinimalEffortSortings() {
			return new String[0];
		}

		//Iterator implementation
		private class InternalIterator extends BPRowIterator{
			private final BPRowKeyBufferIterator sourceIter;
			private final boolean                needReSort;

			InternalIterator (BPRowIterator sourceIter, boolean needReSort){
				this.sourceIter    = new BPRowKeyBufferIterator(sourceIter, keyFieldPositions);
				this.needReSort    = needReSort;
			}

			public String[] getSorting() {
				return sourceIter.getSorting();
			}

			protected BPRow [] internalFetch(int suggestedSize){
				BPRow [] inBuffer = sourceIter.nextKey();
				if (inBuffer != null && inBuffer.length != 0){
					return processBufferedKey(inBuffer);
				}
				return null;
			}

			private BPRow[] processBufferedKey(BPRow [] data){
				ArrayList<BPRow> bufferCreator = new ArrayList<BPRow>();
				if (needReSort){
					Arrays.sort(data, BPRow.NO_VALUE_COMPARATOR);
				}
				int fromDate = data[0].startIncl;
				int bufferSize;
				for (bufferSize = 0; bufferSize < data.length && data[bufferSize].startIncl == fromDate; bufferSize++);

				for (int bufferLoc = bufferSize; bufferLoc<data.length; bufferLoc++){
					BPRow b = data[bufferLoc];
					while (bufferSize != 0 && b.startIncl >= data[bufferSize-1].end){
						BPRow calc     = evaluate(data, bufferSize);
						int enddate    = data[bufferSize-1].end;
						calc.startIncl = fromDate;
						calc.end       = enddate;
						fromDate       = enddate;
						bufferCreator.add(calc);

						while (bufferSize != 0 && enddate == data[bufferSize-1].end)
							bufferSize--;
					}

					boolean done = false;
					for (int i=bufferSize; !done; i--){
						if (i>0 && data[i-1].end < b.end){
							data[i] = data[i-1];
						}else{
							done = true;
							data[i] = b;
							bufferSize++;
						}
					}
					if (bufferSize == 1)
						fromDate = b.startIncl;
				}
				while (bufferSize != 0){
					BPRow calc     = evaluate(data, bufferSize);
					int enddate    = data[bufferSize-1].end;
					calc.startIncl = fromDate;
					calc.end       = enddate;
					fromDate       = enddate;
					bufferCreator.add(calc);

					while (bufferSize != 0 && enddate == data[bufferSize-1].end)
						bufferSize--;
				}
				return bufferCreator.toArray(new BPRow[0]);
			}

			private BPRow evaluate (BPRow[] input, int size){
				return projFilter.convert(BPRow.shrinkArray(input, size));
			}
		}
	}
}
