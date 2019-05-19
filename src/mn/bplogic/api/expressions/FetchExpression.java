package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRowSource;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;

public class FetchExpression {
	public static NumberExpression getInstance(String label, BPRowSource input){
		try{
			return new IntFetchValue(label, input);
		} catch (UnknownColumnException e){
			return new DoubleFetchValue(label, input);
		}
	}
}
