package mn.bplogic.rowsources;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mn.bplogic.api.ExpressionFilterAndProjector;
import mn.bplogic.api.RegularConverter;
import mn.bplogic.api.expressions.DoubleFetchValue;
import mn.bplogic.api.expressions.IntFetchValue;
import mn.bplogic.api.expressions.NumberExpression;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;

public class BPMergeJoinRowSource extends BPBufferRowSource {
	final public static int INNER_JOIN       = 0;
	final public static int LEFT_OUTER_JOIN  = 1;
	final public static int RIGHT_OUTER_JOIN = 2;
	final public static int FULL_OUTER_JOIN  = 3;

	public BPMergeJoinRowSource(String rowSourceName, BPRowSource leftSource, BPRowSource rightSource, String[] leftKeys) {
		this(rowSourceName, leftSource, rightSource, leftKeys, leftKeys, INNER_JOIN);
	}
	public BPMergeJoinRowSource(String rowSourceName, BPRowSource leftSource, BPRowSource rightSource, String[] leftKeys, String [] rightKeys, int joinType) {
		this(rowSourceName, leftSource, rightSource, leftKeys, rightKeys, new ExpressionFilterAndProjector(createFullExpressionMap(leftSource, rightSource, leftKeys)), joinType, true);
	}
	private static Map<String, NumberExpression> createFullExpressionMap(BPRowSource leftSource, BPRowSource rightSource, String [] rightKeys) {
		Map<String, NumberExpression> output = new HashMap<String, NumberExpression>();
		String leftAlias  = leftSource.rowSourceName.toUpperCase().replaceAll(" ", "_");
		String rightAlias = rightSource.rowSourceName.toUpperCase().replaceAll(" ", "_");
		for (String s : leftSource.intFieldLabels)
			output.put(leftAlias + "." + s, new IntFetchValue(s, leftSource));
		for (String s : rightSource.intFieldLabels)
			output.put(rightAlias + "." + s, new IntFetchValue(s, rightSource, leftSource.intFieldLabels.length));

		for (String s : leftSource.doubleFieldLabels)
			output.put(leftAlias + "." + s, new DoubleFetchValue(s, leftSource));
		for (String s : rightSource.doubleFieldLabels)
			output.put(rightAlias + "." + s, new DoubleFetchValue(s, rightSource, leftSource.doubleFieldLabels.length));

		return output;
	}
	public BPMergeJoinRowSource(String rowSourceName, BPRowSource leftSource, BPRowSource rightSource, String[] leftKeys, String [] rightKeys, RegularConverter projFilter) {
		this(rowSourceName, leftSource, rightSource, leftKeys, rightKeys, projFilter, INNER_JOIN, false);
	}
	public BPMergeJoinRowSource(String rowSourceName, BPRowSource leftSource, BPRowSource rightSource, String[] leftKeys, String [] rightKeys, RegularConverter projFilter, int joinType, boolean applyGlueAndPaste) {
		super();
		BPRowSource internalRS = new BPMergeJoinRowSourceInternal(rowSourceName, leftSource, rightSource, leftKeys, rightKeys, projFilter, joinType, applyGlueAndPaste);
		init(internalRS, BUFFER_ON_FIRST_FETCH);
	}


	class BPMergeJoinRowSourceInternal extends BPRowSource {
		private final BPRowSource      leftSource;
		private final BPRowSource      rightSource;
		private final String []        leftKeys;
		private final String []        rightKeys;
		private final int []           leftKeyPositions;
		private final int []           rightKeyPositions;
		private final int              joinType;

		private final RegularConverter projFilter;
		private final boolean          applyGlueAndPaste;
		private final int              leftInts;
		private final int              rightInts;
		private final int              leftDoubles;
		private final int              rightDoubles;

		BPMergeJoinRowSourceInternal(String rowSourceName, BPRowSource leftSource, BPRowSource rightSource, String[] leftKeys, String [] rightKeys, RegularConverter projFilter, int joinType, boolean applyGlueAndPaste){
			if (leftKeys.length != rightKeys.length)
				throw new IllegalArgumentException("Join key length doesn't match");

			this.rowSourceName     = rowSourceName;
			this.leftSource        = leftSource;
			this.rightSource       = rightSource;
			this.leftInts          = leftSource.getIntFieldLabels().length;
			this.rightInts         = rightSource.getIntFieldLabels().length;
			this.leftDoubles       = leftSource.getDoubleFieldLabels().length;
			this.rightDoubles      = rightSource.getDoubleFieldLabels().length;
			this.leftKeys          = leftKeys;
			this.rightKeys         = rightKeys;
			this.joinType          = joinType;
			this.projFilter        = projFilter;
			this.intFieldLabels    = projFilter.getIntFieldLabels();
			this.doubleFieldLabels = projFilter.getDoubleFieldLabels();
			this.intTypes          = projFilter.getIntTypes();
			this.applyGlueAndPaste = applyGlueAndPaste;
			this.leftKeyPositions  = getPositions(leftSource,  leftKeys);
			this.rightKeyPositions = getPositions(rightSource, rightKeys);
		}

		private int[] getPositions(BPRowSource source, String[] keys) {
			int [] output = new int [keys.length];
			HashMap<String, Integer>  findLocMap            = new HashMap<String, Integer>();
			for (int i=0; i< source.intFieldLabels.length; i++){
				findLocMap.put(source.intFieldLabels[i], i);
			}
			for (int i=0; i< source.doubleFieldLabels.length; i++){
				findLocMap.put(source.doubleFieldLabels[i], -1-i);
			}
			int i = 0;
			for (String s : keys){
				Integer k = findLocMap.get(s);
				if (k == null){
					throw new UnknownColumnException(s, source.rowSourceName);
				}
				output[i++] = k;
			}
			return output;
		}

		@SuppressWarnings("unused")
		public BPRowIterator getPredicatedIterator(int effort, String[] predicateColumns, int[] predicateValues, String[] subSortColumns) {
			// Since this may return null in certain cases even if effort = FORCE_RESULT a surrounding buffer is required
			if (predicateColumns != null || predicateValues != null || subSortColumns != null)
				return null;

			// Determine good sources for efficient join (refrain from creating extra sorts on longest row source)
			BPRowIterator sourceIterLeft;
			BPRowIterator sourceIterRight;
			boolean leftIsBiggest   = leftSource.deepRowCount() > rightSource.deepRowCount();
			boolean leftNeedsResort = false;
			boolean rightNeedsResort = false;

			if (false){
				if (leftIsBiggest){
					// first determine left iterator
					BPRowIterator leftOpt  = leftSource.getPredicatedIterator(MINIMAL_EFFORT, null,     null, leftKeys);
					BPRowIterator leftEasy = leftSource.getPredicatedIterator(MINIMAL_EFFORT, leftKeys, null, null);
					if (leftOpt == null && leftEasy == null){
						sourceIterLeft = leftSource .getPredicatedIterator(effort, null, null, leftKeys);
					} else if (leftOpt != null){
						sourceIterLeft = leftOpt;
					} else {
						sourceIterLeft  = leftEasy;
						leftNeedsResort = true;
					}
					if (sourceIterLeft != null){
						// now get right iterator
						String [] rightKeysOrdered = translateKeys(leftKeys, sourceIterLeft.getSorting(), rightKeys);
						sourceIterRight = rightSource.getPredicatedIterator(effort, null, null, rightKeysOrdered);
					}
				}
			}

			sourceIterLeft  = leftSource .getPredicatedIterator(effort, null, null, leftKeys);
			sourceIterRight = rightSource.getPredicatedIterator(effort, null, null, rightKeys);
			leftNeedsResort = false;
			if (sourceIterLeft == null || sourceIterRight == null)
				return null;

			return new InternalIterator(sourceIterLeft,  leftNeedsResort,
					sourceIterRight, rightNeedsResort);
		}

		private String[] translateKeys(String[] orig1, String[] resort1, String[] orig2) {
			String [] output = new String[orig1.length];
			for (int i=0; i<orig1.length; i++)
				for (int j=0; j< resort1.length; j++)
					if (resort1[j].equals(orig1[i]))
						output[j] = orig2[i];

			return output;
		}

		public int deepRowCount() {
			return leftSource.deepRowCount() + rightSource.deepRowCount();
		}
		public String[] getMinimalEffortSortings() {
			return new String[0];
		}

		//Iterator implementation
		private class InternalIterator extends BPRowIterator{
			final private BPRowKeyBufferIterator leftIter;
			final private BPRowKeyBufferIterator rightIter;
			final private boolean                leftNeedsResort;
			final private boolean                rightNeedsResort;
			private BPRow []                     leftBuffer;
			private boolean                      endOfLeft;
			private BPRow []                     rightBuffer;
			private boolean                      endOfRight;

			InternalIterator (BPRowIterator leftIter, boolean leftNeedsResort,
					BPRowIterator rightIter, boolean rightNeedsResort){
				this.leftIter        = new BPRowKeyBufferIterator(leftIter,  leftKeyPositions);
				this.leftNeedsResort = leftNeedsResort;
				this.rightIter       = new BPRowKeyBufferIterator(rightIter, rightKeyPositions);
				this.rightNeedsResort= rightNeedsResort;
				this.endOfLeft       = false;
				this.leftBuffer      = null;
				this.endOfRight      = false;
				this.rightBuffer     = null;
			}

			// Find relative positions of left and right rows
			private int compareJoinKeys(BPRow left, BPRow right){
				for (int i=0; i<leftKeyPositions.length; i++){
					int l = leftKeyPositions[i];
					int r = rightKeyPositions[i];
					if (l>=0 && r >=0){
						if (left.intValues[l] < right.intValues[r])
							return -1;
						else if (left.intValues[l] > right.intValues[r])
							return 1;
					} else if (l<0 && r <0){
						if (left.doubleValues[-l-1] < right.doubleValues[-r-1])
							return -1;
						else if (left.doubleValues[-l-1] > right.doubleValues[-r-1])
							return 1;
					} else if (l<0 && r >=0){
						if (left.doubleValues[-l-1] < right.intValues[r])
							return -1;
						else if (left.doubleValues[-l-1] > right.intValues[r])
							return 1;
					} else {
						if (left.intValues[l] < right.doubleValues[-r-1])
							return -1;
						else if (left.intValues[l] > right.doubleValues[-r-1])
							return 1;
					}
				}
				return 0;
			}

			protected BPRow [] internalFetch(int suggestedSize){
				leftBuffer  = (leftBuffer  == null) ? leftIter.nextKey()  : leftBuffer;
				rightBuffer = (rightBuffer == null) ? rightIter.nextKey() : rightBuffer;
				endOfLeft   = (leftBuffer  == null);
				endOfRight  = (rightBuffer == null);
				BPRow [] output = null;

				if (endOfLeft && endOfRight){
					return null;
				} else if (endOfLeft){
					if (joinType == RIGHT_OUTER_JOIN ||  joinType == FULL_OUTER_JOIN){
						output = processKeyRightOuterJoin(rightBuffer);
						rightBuffer = null;
					} else {
						return null;
					}
				} else if (endOfRight){
					if (joinType == LEFT_OUTER_JOIN  ||  joinType == FULL_OUTER_JOIN){
						output = processKeyLeftOuterJoin(leftBuffer);
						leftBuffer = null;
					} else {
						return null;
					}
				} else { // both sources have rows available
					int comp = compareJoinKeys(leftBuffer[0], rightBuffer[0]);
					if        (comp == -1){
						if (joinType == LEFT_OUTER_JOIN  ||  joinType == FULL_OUTER_JOIN)
							output = processKeyLeftOuterJoin(leftBuffer);
						leftBuffer = null;
					} else if (comp == 1){
						if (joinType == RIGHT_OUTER_JOIN ||  joinType == FULL_OUTER_JOIN)
							output = processKeyRightOuterJoin(rightBuffer);
						rightBuffer = null;
					} else if (comp == 0){
						output = processSameKeyJoin(leftBuffer, rightBuffer);
						leftBuffer  = null;
						rightBuffer = null;
					}
				}
				return output;
			}


			private BPRow[] processKeyLeftOuterJoin(BPRow [] leftRows){
				for (BPRow b : leftRows)
					b.extend(rightInts, rightDoubles, true);
				BPRow[] outBuffer = projFilter.convert(leftRows);
				if (applyGlueAndPaste)
					outBuffer    = BPRow.applyGlueAndPaste(outBuffer, BPRow.GLUE_AND_PASTE_NOSORT, false);
				return outBuffer;
			}
			private BPRow[] processKeyRightOuterJoin(BPRow [] rightRows){
				for (BPRow b : rightRows)
					b.extend(leftInts, leftDoubles, false);
				BPRow[] outBuffer = projFilter.convert(rightRows);
				if (applyGlueAndPaste)
					outBuffer    = BPRow.applyGlueAndPaste(outBuffer, BPRow.GLUE_AND_PASTE_NOSORT, false);
				return outBuffer;
			}

			private BPRow[] processSameKeyJoin(BPRow [] lefts, BPRow [] rights){
				if (leftNeedsResort){
					Arrays.sort(lefts, BPRow.NO_VALUE_COMPARATOR);
				}
				if (rightNeedsResort){
					Arrays.sort(rights, BPRow.NO_VALUE_COMPARATOR);
				}
				List<BPRow> bufferStorage       = new ArrayList<BPRow>();
				if (joinType != INNER_JOIN){
					// fill gaps for outer join results;
					BPIntervalCollection til = new BPIntervalCollection(lefts);
					BPIntervalCollection tir = new BPIntervalCollection(rights);

					til.minus(tir);
					tir.minus(til);
					if (joinType != LEFT_OUTER_JOIN && !tir.isEmpty()){
						BPRow [] additionalLefts = tir.asOJArray(leftInts, leftDoubles);
						BPRow [] leftsOld        = lefts;
						lefts  = new BPRow[leftsOld.length + additionalLefts.length];
						System.arraycopy(leftsOld,        0, lefts, 0,               leftsOld.length);
						System.arraycopy(additionalLefts, 0, lefts, leftsOld.length, additionalLefts.length);
						Arrays.sort(lefts, BPRow.NO_VALUE_COMPARATOR);
					}
					if (joinType != RIGHT_OUTER_JOIN && !til.isEmpty()){
						BPRow [] additionalRights= til.asOJArray(rightInts, rightDoubles);
						BPRow [] rightsOld       = rights;
						rights  = new BPRow[rightsOld.length + additionalRights.length];
						System.arraycopy(rightsOld,        0, rights, 0,                rightsOld.length);
						System.arraycopy(additionalRights, 0, rights, rightsOld.length, additionalRights.length);
						Arrays.sort(rights, BPRow.NO_VALUE_COMPARATOR);
					}
				}



				// Now we have filled for outer joins, execute inner join
				int bufferSize = 0;
				int bufferLoc  = 0;
				BPRow r = bufferLoc < rights.length ? rights[bufferLoc++] : null;

				for(BPRow l : lefts){
					// throw away non matching records
					while (bufferSize != 0 && l.startIncl > rights[bufferSize-1].end)
						bufferSize--;

					// add matching records
					while(r != null && r.startIncl < l.end){
						boolean done = false;
						for (int i=bufferSize; !done; i--){
							if (i>0 && rights[i-1].end < r.end){
								rights[i] = rights[i-1];
							}else{
								done = true;
								rights[i] = r;
								bufferSize++;
							}
						}
						r = bufferLoc < rights.length ? rights[bufferLoc++] : null;
					}

					// execute join
					for (int j = 0; j< bufferSize; j++){
						BPRow out = BPRow.join(l, rights[j]);
						if (out != null){
							bufferStorage.add(out);
						}
					}
				}

				BPRow[] outBuffer = projFilter.convert(bufferStorage.toArray(new BPRow[0]));
				if (applyGlueAndPaste)
					outBuffer    = BPRow.applyGlueAndPaste(outBuffer, BPRow.GLUE_AND_PASTE_NOSORT, false);
				return outBuffer;
			}


			public String[] getSorting() {
				return leftIter.getSorting();
			}

		}
	}
}
