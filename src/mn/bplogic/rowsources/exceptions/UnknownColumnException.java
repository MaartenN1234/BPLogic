package mn.bplogic.rowsources.exceptions;


public class UnknownColumnException extends DataModelException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1284359722739798266L;

	public UnknownColumnException(String s) {
		super("Unknown column " + s);
	}

	public UnknownColumnException(String s, String rowSourceName) {
		super("Unknown column " + s + " in rowsource " + rowSourceName);
	}

}
