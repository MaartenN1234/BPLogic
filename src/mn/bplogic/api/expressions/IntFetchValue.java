package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;

public class IntFetchValue implements IntExpression{
	final private int index;
	final private int intType;
	public IntFetchValue(String column, String [] intLabels, int[] intTypes){
		this(column, intLabels, intTypes, 0);
	}
	public IntFetchValue(String column, String [] intLabels, int[] intTypes, int indexOffset){
		int i = 0;
		for (String s : intLabels){
			if (s.equals(column)){
				index   = i+indexOffset;
				intType = intTypes[i];
				return;
			}
			i++;
		}
		throw new UnknownColumnException(column);
	}

	public IntFetchValue(String column, BPRowSource bpTable) {
		this(column, bpTable, 0);
	}
	public IntFetchValue(String column, BPRowSource bpTable, int indexOffset) {
		this(column, bpTable.getIntFieldLabels(), bpTable.getIntTypes(), indexOffset);
	}

	public int[] evaluate(BPRow[] inputs){
		int[] output = new int[inputs.length];
		int j = 0;
		for (BPRow input : inputs){
			output[j++] = input.intValues[index];
		}

		return output;
	}
	public int getIntType() {
		return intType;
	}
	public int getIndex(){
		return index;
	}
}
