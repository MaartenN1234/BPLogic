package mn.bplogic.rowsources;

public abstract class BPRowIterator {
	public    abstract String [] getSorting();
	protected abstract BPRow  [] internalFetch(int suggestedSize);

	private BPRow[] buffer;
	private int     bufferLoc;
	private boolean endOfIterator;

	public BPRow next(){
		BPRow [] buf = nextBuffer(1);
		if (buf.length >0)
			return buf[0];
		return null;
	}

	public BPRow[] nextBuffer(int bufferSize) {
		if (buffer == null){
			buffer        = internalFetch(bufferSize);
			bufferLoc     = 0;
			endOfIterator = (buffer == null || buffer.length ==0);
		}

		BPRow [] output = new BPRow[bufferSize];
		int i=0;
		while(i < bufferSize && !endOfIterator){
			int fetchedSize  = buffer.length - bufferLoc;
			int todoSize     = bufferSize - i;
			int transferSize = fetchedSize > todoSize ? todoSize : fetchedSize;
			System.arraycopy(buffer, bufferLoc, output, i, transferSize);
			i         += transferSize;
			bufferLoc += transferSize;

			if (bufferLoc == buffer.length){
				buffer    = internalFetch(bufferSize);
				bufferLoc = 0;
				endOfIterator =  (buffer == null || buffer.length ==0);
			}
		}

		if (endOfIterator)
			output = BPRow.shrinkArray(output, i);

		return output;
	}}
