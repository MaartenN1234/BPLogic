package mn.bplogic.rowsources;

import java.util.ArrayList;

import mn.bplogic.main.Settings;

public class BPRowKeyBufferIterator extends BPRowIterator {
	final private BPRowIterator source;
	final private int []        keyIndexes;
	private ArrayList<BPRow[]>  fetchBuffer;
	private int                 firstBufferLoc;
	private boolean             endOfSourceIterator;
	private boolean             endOfIterator;

	public BPRowKeyBufferIterator(BPRowIterator source, int[] keyIndexes){
		this.source         = source;
		this.keyIndexes     = keyIndexes;
		this.fetchBuffer    = new ArrayList<BPRow[]>();
		this.firstBufferLoc = 0;
		this.endOfIterator  = false;
		this.endOfSourceIterator=false;
	}

	// normal iterator procedures (passthrough to source directly)
	public String[] getSorting() {
		return source.getSorting();
	}
	protected BPRow[] internalFetch(int suggestedSize) {
		return source.internalFetch(suggestedSize);
	}

	// specific procedure
	public BPRow[] nextKey(){
		if (endOfIterator)
			return null;

		final int fetchSize = Settings.InternalFetchSize;

		if (fetchBuffer.size() == 0){
			BPRow [] temp = source.nextBuffer(fetchSize);
			if (temp != null && temp.length >0){
				fetchBuffer.add(temp);
			} else {
				endOfSourceIterator = true;
			}
		}
		BPRow referenceRow     = fetchBuffer.get(0)[firstBufferLoc];
		BPRow [] temp          = fetchBuffer.get(fetchBuffer.size()-1);
		BPRow lastAvailableRow = temp[temp.length-1];
		while (!endOfSourceIterator && haveEqualKeys(referenceRow, lastAvailableRow)){
			temp = source.nextBuffer(fetchSize);
			if (temp != null && temp.length >0){
				lastAvailableRow = temp[temp.length-1];
				fetchBuffer.add(temp);
			} else {
				fetchBuffer.add(temp);
				endOfSourceIterator = true;
			}
		}


		if (haveEqualKeys(referenceRow, lastAvailableRow)){
			// flush all remaining

			// fetch Values
			BPRow [] output = fetchUntilIndexExcluding(temp.length);
			// set status correct
			endOfIterator = true;
			return output;
		} else {
			// flush until next key switch
			int j = (fetchBuffer.size() == 1) ? firstBufferLoc + 1 : 0;
			for (int k=j; k<temp.length; k++){
				BPRow testRow = temp[k];
				if (!haveEqualKeys(referenceRow, testRow)){
					// fetch Values
					BPRow [] output = fetchUntilIndexExcluding(k);
					return output;
				}
			}
		}
		// should not reach this stage
		throw new RuntimeException ("Assertion failed, both no keyswitch and keyswitch detected");
	}

	// help method compare
	private boolean haveEqualKeys(BPRow reference, BPRow test){
		for (int i : keyIndexes){
			if (i>=0){
				if (reference.intValues[i] != test.intValues[i])
					return false;
			} else {
				if (reference.doubleValues[-i-1] != test.doubleValues[-i-1])
					return false;
			}
		}
		return true;
	}
	// help method create Buffer
	private BPRow[] fetchUntilIndexExcluding(int beyondIndexLastBuffer) {
		int loc = 0;
		for (int i = 0; i<fetchBuffer.size(); i++){
			int length = 0;
			if ((i==0) && (i == (fetchBuffer.size() - 1))){
				length = beyondIndexLastBuffer - firstBufferLoc;
			} else if (i==0){
				length = fetchBuffer.get(i).length - firstBufferLoc;
			} else if (i == (fetchBuffer.size() - 1)){
				length = beyondIndexLastBuffer;
			} else {
				length = fetchBuffer.get(i).length;
			}
			loc += length;
		}
		BPRow [] tempData = new BPRow[loc];

		loc = 0;
		for (int i = 0; i<fetchBuffer.size(); i++){
			int length = 0;
			if ((i==0) && (i == (fetchBuffer.size() - 1))){
				length = beyondIndexLastBuffer - firstBufferLoc;
			} else if (i==0){
				length = fetchBuffer.get(i).length - firstBufferLoc;
			} else if (i == (fetchBuffer.size() - 1)){
				length = beyondIndexLastBuffer;
			} else {
				length = fetchBuffer.get(i).length;
			}
			int fetchBufferLoc = (i==0) ? firstBufferLoc : 0;
			System.arraycopy(fetchBuffer.get(i), fetchBufferLoc, tempData, loc, length);
			loc += length;
		}
		BPRow [] lastBuffer = fetchBuffer.get(fetchBuffer.size()-1);
		firstBufferLoc = beyondIndexLastBuffer;
		fetchBuffer.clear();
		fetchBuffer.add(lastBuffer);

		return tempData;
	}
}
