package mn.bplogic.api.expressions;

import java.util.ArrayList;

import mn.bplogic.rowsources.BPRow;

public class DoubleSum implements DoubleExpression {
	private IntExpression    [] neIA;
	private DoubleExpression [] neDA;

	public DoubleSum (NumberExpression [] neA){
		ArrayList<IntExpression>    neIAA = new ArrayList<IntExpression>();
		ArrayList<DoubleExpression> neDAA = new ArrayList<DoubleExpression>();
		for (NumberExpression ne : neA){
			if (ne instanceof IntExpression){
				neIAA.add((IntExpression) ne);
			} else if (ne instanceof DoubleExpression){
				neDAA.add((DoubleExpression) ne);
			}else{
				throw new RuntimeException("Unknown implementation of NumberExpression");
			}
		}

		neIA = neIAA.toArray(new IntExpression[0]);
		neDA = neDAA.toArray(new DoubleExpression[0]);
	}


	public double[] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		double [] output = new double[size];
		for (int i=0; i <size; i++){
			output[i] = 0;
		}
		for (IntExpression ie  : neIA){
			int [] tmp = ie.evaluate(inputs);
			for (int i=0; i <size; i++){
				output[i] += tmp[i];
			}
		}
		for (DoubleExpression ie  : neDA){
			double [] tmp = ie.evaluate(inputs);
			for (int i=0; i <size; i++){
				output[i] += tmp[i];
			}
		}
		return output;
	}
}
