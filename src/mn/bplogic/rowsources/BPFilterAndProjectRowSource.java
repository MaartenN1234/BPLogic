package mn.bplogic.rowsources;

import java.util.Arrays;
import java.util.HashSet;

import mn.bplogic.api.RegularConverter;

public class BPFilterAndProjectRowSource extends BPBufferRowSource {

	public BPFilterAndProjectRowSource(String rowSourceName, BPRowSource source, RegularConverter projFilter) {
		this(rowSourceName, source, projFilter, false);
	}
	public BPFilterAndProjectRowSource(String rowSourceName, BPRowSource source, RegularConverter projFilter, boolean applyGlueAndPaste) {
		super();
		BPRowSource internalRS = new BPFilterAndProjectorRowSourceInternal(rowSourceName, source, projFilter, applyGlueAndPaste);
		init(internalRS, BUFFER_AS_LAST_RESORT_ONLY);
	}


	class BPFilterAndProjectorRowSourceInternal extends BPRowSource {
		protected BPRowSource source;
		protected RegularConverter projFilter;
		protected HashSet<String> sourceFieldLabels;

		final private boolean applyGlueAndPaste;

		BPFilterAndProjectorRowSourceInternal(String rowSourceName, BPRowSource source, RegularConverter projFilter, boolean applyGlueAndPaste){
			this.rowSourceName     = rowSourceName;
			this.source            = source;
			this.sourceFieldLabels = new HashSet<String>();
			this.sourceFieldLabels.addAll(Arrays.asList(source.intFieldLabels));
			this.sourceFieldLabels.addAll(Arrays.asList(source.doubleFieldLabels));
			this.projFilter        = projFilter;
			this.intFieldLabels    = projFilter.getIntFieldLabels();
			this.doubleFieldLabels = projFilter.getDoubleFieldLabels();
			this.intTypes          = projFilter.getIntTypes();
			this.applyGlueAndPaste = applyGlueAndPaste;

		}
		public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValues, String[] subSortColumns) {
			// Check column existance in source as columns may only be created during this projection
			// Since this may return null in certain cases even if effort = FORCE_RESULT a surrounding buffer is required
			if (predicateColumns != null){
				for (String s : predicateColumns){
					if (!(sourceFieldLabels.contains(s))){
						return null;
					}
				}
			}
			if (subSortColumns != null){
				for (String s : subSortColumns){
					if (!(sourceFieldLabels.contains(s))){
						return null;
					}
				}
			}
			BPRowIterator sourceIter = source.getPredicatedIterator(effort, predicateColumns, predicateValues, subSortColumns);
			if (sourceIter == null)
				return null;
			return new InternalIterator(sourceIter);
		}

		public int deepRowCount() {
			return source.deepRowCount();
		}
		public String[] getMinimalEffortSortings() {
			return source.getMinimalEffortSortings();
		}


		//Iterator implementation
		private class InternalIterator extends BPRowIterator{
			private final BPRowIterator sourceIter;

			InternalIterator (BPRowIterator sourceIter){
				this.sourceIter = sourceIter;
			}

			protected BPRow[] internalFetch(int suggestedSize){
				BPRow [] inBuffer  = sourceIter.nextBuffer(suggestedSize);
				BPRow [] outBuffer = null;
				if (inBuffer != null && inBuffer.length != 0){
					outBuffer = projFilter.convert(inBuffer);

					if (applyGlueAndPaste && outBuffer.length != 0){
						outBuffer = BPRow.applyGlueAndPaste(outBuffer, BPRow.GLUE_AND_PASTE_NOSORT, false);
					}
					if (outBuffer.length == 0){
						return internalFetch(suggestedSize);
					}
				}
				return outBuffer;
			}

			public String[] getSorting() {
				return sourceIter.getSorting();
			}

		}
	}
}
