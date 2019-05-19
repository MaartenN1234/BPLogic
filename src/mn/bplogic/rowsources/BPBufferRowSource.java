package mn.bplogic.rowsources;

import java.lang.ref.WeakReference;
import java.util.ArrayList;

import mn.bplogic.main.MemoryStatics;
import mn.bplogic.main.MemoryStaticsReporterMessage;
import mn.bplogic.main.Settings;

public class BPBufferRowSource extends BPRowSource {
	public static int BUFFER_ON_INIT              = 0;
	public static int BUFFER_ON_FIRST_FETCH       = 1;
	public static int BUFFER_AS_LAST_RESORT_ONLY  = 9;

	BPTable     buffer = null;
	BPRowSource source = null;
	WeakReference<BPRowSource> sourceWeakRef = null;
	int         bufferLikelihood;

	final double factorBenefitBeforeExtraSort = 0.3;

	public BPBufferRowSource (){
	}
	public BPBufferRowSource (BPRowSource source){
		this(source, BUFFER_ON_FIRST_FETCH);
	}
	public BPBufferRowSource (BPRowSource source, int bufferLikelihood){
		init(source, bufferLikelihood);
	}

	protected void init(BPRowSource source, int bufferLikelihood){
		this.source              = source;
		this.intFieldLabels      = source.intFieldLabels;
		this.doubleFieldLabels   = source.doubleFieldLabels;
		this.intTypes            = source.intTypes;
		this.rowSourceName       = source.rowSourceName + " (BUFFERED)";
		this.bufferLikelihood    = bufferLikelihood;
		if (bufferLikelihood <= BUFFER_ON_INIT){
			createBuffer();
		}
	}

	protected int getRowCountFast(){
		if (rowCount == -1){
			if(bufferLikelihood <= BUFFER_ON_FIRST_FETCH){
				rowCount = source.getRowCount(false);
				if (rowCount == -1)
					createBuffer();
			} else {
				rowCount = source.getRowCount(true);
			}
		}
		return rowCount;
	}
	public int deepRowCount(){
		if (buffer == null)
			return source.deepRowCount();
		return buffer.deepRowCount();
	}
	public String[] getMinimalEffortSortings() {
		if (buffer == null)
			return source.getMinimalEffortSortings();
		return buffer.getMinimalEffortSortings();
	}

	protected void createBuffer(){
		BPRowIterator bpIter = source.getIterator();
		if (bpIter == null){
			throw new RuntimeException("Source for buffer does not supply an unconditional iterator.");
		}
		createBuffer(bpIter, true);
	}
	protected void createBuffer(BPRowIterator bpIter){
		createBuffer(bpIter, false);
	}
	protected void createBuffer(BPRowIterator bpIter, boolean forceSortedGlueAndPaste){
		long timeStart = System.currentTimeMillis();
		final int fetchSize = Settings.InternalFetchSize;
		rowCount = 0;

		ArrayList<BPRow[]> prepDataArr = new ArrayList<BPRow[]>();
		BPRow[]            prepData    = bpIter.nextBuffer(fetchSize);
		while (prepData.length > 0){
			prepDataArr.add(prepData);
			rowCount   += prepData.length;
			prepData    = bpIter.nextBuffer(fetchSize);
		}
		BPRow[] data = new BPRow[rowCount];
		int destPos = 0;
		for (BPRow[] bpra : prepDataArr){
			System.arraycopy(bpra, 0, data, destPos, bpra.length);
			destPos += fetchSize;
		}

		BPRow[]  gluedData;
		String[] finalSortLabels;
		boolean sortedGlueAndPaste = forceSortedGlueAndPaste || bpIter.getSorting() == null || bpIter.getSorting().length == 0;

		if (sortedGlueAndPaste){
			if (bpIter.getSorting() == null || bpIter.getSorting().length == 0){
				gluedData       = BPRow.applyGlueAndPaste(data, BPRow.GLUE_AND_PASTE_SORT_SINGLE_FILTER, false);
			} else {
				gluedData       = BPRow.applyGlueAndPaste(data, BPRow.GLUE_AND_PASTE_SORT_DUAL_FILTER, false);
			}
			finalSortLabels = getAllFieldLabels();
		} else {
			gluedData       = BPRow.applyGlueAndPaste(data, BPRow.GLUE_AND_PASTE_NOSORT, false);
			finalSortLabels = bpIter.getSorting();
		}
		rowCount = gluedData.length;
		long endStart = System.currentTimeMillis();

		MemoryStatics.register(new MemoryStaticsReporterMessage("BufferRowsource "+ rowSourceName+"\t\tFetchtime: "+(endStart-timeStart)+"ms"));
		buffer   = new BPTable(rowSourceName, intTypes, intFieldLabels, doubleFieldLabels, gluedData, finalSortLabels);

		// now the buffer is in place make the source available for Garbage Collection
		sourceWeakRef = new WeakReference<BPRowSource>(source);
		source        = null;
	}



	public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValues, String[] subSortColumns) {
		BPRowIterator tryGetIter;
		if (buffer == null){
			boolean createBuffer = false;

			if(bufferLikelihood <= BUFFER_ON_FIRST_FETCH){
				createBuffer = true;
			} else if (effort==FORCE_RESULT){
				// Catch FORCE_RESULT effort by creating buffer
				tryGetIter = source.getPredicatedIterator(effort, predicateColumns, predicateValues, subSortColumns);
				if (tryGetIter != null){
					return tryGetIter;
				} else {
					createBuffer = true;
				}
			}

			if (createBuffer){
				tryGetIter = source.getPartitionedIterator(MINIMAL_EFFORT, predicateColumns, subSortColumns);
				if (tryGetIter == null){
					createBuffer();
				} else {
					createBuffer(tryGetIter);
				}
			} else {
				return source.getPredicatedIterator(effort, predicateColumns, predicateValues, subSortColumns);
			}
		}

		if (effort==FORCE_RESULT){
			tryGetIter = buffer.getPredicatedIterator(MINIMAL_EFFORT, predicateColumns, predicateValues, subSortColumns);
			if (tryGetIter == null && bufferLikelihood > BUFFER_ON_FIRST_FETCH){
				BPRowSource src = sourceWeakRef.get();
				if(src != null && (factorBenefitBeforeExtraSort * src.deepRowCount()) < buffer.deepRowCount()){
					tryGetIter = src.getPredicatedIterator(MINIMAL_EFFORT, predicateColumns, predicateValues, subSortColumns);
					if (tryGetIter != null){
						// Requires sort on buffer, but not on underlying source, return source iterator
						return tryGetIter;
					}
				}
			}
		}

		return buffer.getPredicatedIterator(effort, predicateColumns, predicateValues, subSortColumns);
	}
}
