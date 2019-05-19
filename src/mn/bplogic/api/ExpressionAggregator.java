package mn.bplogic.api;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import mn.bplogic.api.expressions.DoubleExpression;
import mn.bplogic.api.expressions.IntExpression;
import mn.bplogic.api.expressions.NumberExpression;
import mn.bplogic.rowsources.BPRow;

public class ExpressionAggregator implements AggregateConverter {
	final private String[]              intFieldLabels;
	final private String[]              doubleFieldLabels;
	final private int[]                 intTypes;
	final private IntExpression[]       integerOutput;
	final private DoubleExpression[]    doubleOutput;
	final private int nrInts;
	final private int nrDoubles;

	public ExpressionAggregator(Map<String, NumberExpression> outputValues){
		ArrayList<String>           iel = new ArrayList<String>();
		ArrayList<String>           del = new ArrayList<String>();
		ArrayList<IntExpression>    iea = new ArrayList<IntExpression>();
		ArrayList<Integer>          iet = new ArrayList<Integer>();
		ArrayList<DoubleExpression> dea = new ArrayList<DoubleExpression>();

		for (Entry<String, NumberExpression> e : outputValues.entrySet()){
			String label          = e.getKey();
			NumberExpression expr = e.getValue();
			if (expr instanceof DoubleExpression){
				dea.add((DoubleExpression) expr);
				del.add(label);
			} else if (expr instanceof IntExpression){
				iea.add((IntExpression) expr);
				iel.add(label);
				iet.add(((IntExpression) expr).getIntType());
			} else {
				throw new RuntimeException("Unknown implementation of NumberExpression");
			}
		}

		this.integerOutput     = iea.toArray(new IntExpression[0]);
		this.intFieldLabels    = iel.toArray(new String[0]);
		this.doubleOutput      = dea.toArray(new DoubleExpression[0]);
		this.doubleFieldLabels = del.toArray(new String[0]);
		this.intTypes          = new int[integerOutput.length];
		int i = 0;
		for(Integer k : iet){
			intTypes[i++] = k;
		}
		this.nrInts    = integerOutput.length;
		this.nrDoubles = doubleOutput.length;
	}



	public BPRow convert(BPRow[] inputs){
		// project
		BPRow output = new BPRow(nrInts, nrDoubles);
		for (int i = 0; i <integerOutput.length; i++){
			int tmp []  = integerOutput[i].evaluate(inputs);
			output.intValues[i] = tmp[0];
		}
		for (int i = 0; i <doubleOutput.length; i++){
			double [] tmp = doubleOutput[i].evaluate(inputs);
			output.doubleValues[i] = tmp[0];
		}
		// done
		return output;
	}




	public final String[] getIntFieldLabels() {
		return intFieldLabels;
	}
	public final String[] getDoubleFieldLabels() {
		return doubleFieldLabels;
	}
	public final int[] getIntTypes() {
		return intTypes;
	}
}
