package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;
import mn.bplogic.rowsources.exceptions.UnknownColumnException;

public class DoubleFetchValue implements DoubleExpression {
	final  private int index;
	public DoubleFetchValue(String column, String [] doubleLabels){
		this(column, doubleLabels, 0);
	}
	public DoubleFetchValue(String column, String [] doubleLabels, int indexOffset){
		int i = 0;
		for (String s : doubleLabels){
			if (s.equals(column)){
				index = i + indexOffset;
				return;
			}
			i++;
		}
		throw new UnknownColumnException(column);
	}

	public DoubleFetchValue(String column, BPRowSource bpTable){
		this(column, bpTable,0 );
	}
	public DoubleFetchValue(String column, BPRowSource bpTable, int indexOffset) {
		this(column, bpTable.getDoubleFieldLabels(), indexOffset);
	}


	public double[] evaluate(BPRow[] inputs){
		double [] output = new double[inputs.length];
		int j = 0;
		for (BPRow input : inputs){
			output[j++] = input.doubleValues[index];
		}
		return output;
	}

	public int getIndex() {
		return index;
	}
}
