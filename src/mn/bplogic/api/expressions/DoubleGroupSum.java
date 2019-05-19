package mn.bplogic.api.expressions;

import java.util.ArrayList;

import mn.bplogic.rowsources.BPRow;

public class DoubleGroupSum extends DoubleGroupExpression {
	private IntExpression    [] neIA;
	private DoubleExpression [] neDA;

	public DoubleGroupSum (NumberExpression [] neA){
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


	public DoubleGroupSum(NumberExpression hrm) {
		this(new NumberExpression[]{hrm});
	}


	public double evaluateGroup(BPRow[] inputs) {
		double  output = 0;

		for (IntExpression ie  : neIA)
			for (int i : ie.evaluate(inputs))
				output += i;

		for (DoubleExpression de  : neDA)
			for (double d : de.evaluate(inputs))
				output += d;

		return output;
	}
}
