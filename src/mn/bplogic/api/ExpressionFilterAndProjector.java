package mn.bplogic.api;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import mn.bplogic.api.expressions.BooleanExpression;
import mn.bplogic.api.expressions.DoubleExpression;
import mn.bplogic.api.expressions.DoubleFetchValue;
import mn.bplogic.api.expressions.IntExpression;
import mn.bplogic.api.expressions.IntFetchValue;
import mn.bplogic.api.expressions.NumberExpression;
import mn.bplogic.api.expressions.TRUE;
import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class ExpressionFilterAndProjector implements RegularConverter {
	final private String[]           intFieldLabels;
	final private String[]           doubleFieldLabels;
	final private int[]              intTypes;
	final private BooleanExpression  filterCriterium;
	final private IntExpression[]    integerOutput;
	final private DoubleExpression[] doubleOutput;
	final private int                nrInts;
	final private int                nrDoubles;
	final private boolean            projectionPassThrough;
	private boolean                  staticFilterCriterium;
	private boolean                  staticFilterValue;

	public ExpressionFilterAndProjector(BPRowSource passThroughInput){
		this(passThroughInput, TRUE.STATIC);
	}
	public ExpressionFilterAndProjector(BPRowSource passThroughInput, BooleanExpression filterCriterium){
		this.projectionPassThrough = true;

		this.intFieldLabels        = passThroughInput.getIntFieldLabels();
		this.doubleFieldLabels     = passThroughInput.getDoubleFieldLabels();
		this.intTypes              = passThroughInput.getIntTypes();
		this.nrInts                = intFieldLabels.length;
		this.nrDoubles             = doubleFieldLabels.length;
		this.filterCriterium       = filterCriterium;
		this.integerOutput         = null;
		this.doubleOutput          = null;
		initStaticFilterCriterium();
	}
	public ExpressionFilterAndProjector(Map<String, NumberExpression> outputValues){
		this(outputValues, TRUE.STATIC);
	}
	public ExpressionFilterAndProjector(Map<String, NumberExpression> outputValues, BooleanExpression filterCriterium){
		this.projectionPassThrough = false;

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

		this.filterCriterium   = filterCriterium;
		initStaticFilterCriterium();
	}

	private void initStaticFilterCriterium(){
		staticFilterCriterium = false;
		staticFilterValue     = false;
		try{
			staticFilterValue     = filterCriterium.evaluate(new BPRow[]{null})[0];
			staticFilterCriterium = true;
		} catch (NullPointerException e){
			staticFilterCriterium = false;
		}
	}


	public BPRow[] convert(BPRow[] inputs){
		BPRow[] passThrough;
		int size                 = inputs.length;

		// filter
		if (staticFilterCriterium){
			if (staticFilterValue){
				passThrough = inputs;
			} else {
				return new BPRow[0];
			}
		} else {
			boolean [] filterResults = filterCriterium.evaluate(inputs);

			int filterSize = 0;
			for (int i=0; i<size;i++)
				if (filterResults[i])
					filterSize++;
			if (filterSize == 0){
				return new BPRow[0];
			} else if (filterSize == size){
				passThrough = inputs;
			} else {
				passThrough = new BPRow[filterSize];

				int j = 0;
				for (int i=0; i<size;i++)
					if (filterResults[i])
						passThrough[j++] = inputs[i];
				size = filterSize;
			}
		}

		// project
		BPRow [] output = new BPRow[size];
		if (projectionPassThrough){
			for (int i = 0; i<size; i++){
				output[i] = passThrough[i].clone();
			}
		} else {
			for (int i = 0; i<size; i++){
				output[i] = new BPRow(nrInts, nrDoubles);
				output[i].startIncl = passThrough[i].startIncl;
				output[i].end       = passThrough[i].end;
			}
			for (int i = 0; i <integerOutput.length; i++){
				if (integerOutput[i] instanceof IntFetchValue){
					int index = ((IntFetchValue) (integerOutput[i])).getIndex();
					for (int j=0; j<size; j++)
						output[j].intValues[i] = passThrough[j].intValues[index];
				} else {
					int tmp []  = integerOutput[i].evaluate(passThrough);
					for (int j=0; j<size; j++)
						output[j].intValues[i] = tmp[j];
				}
			}
			for (int i = 0; i <doubleOutput.length; i++){
				if (doubleOutput[i] instanceof DoubleFetchValue){
					int index = ((DoubleFetchValue) (doubleOutput[i])).getIndex();
					for (int j=0; j<size; j++)
						output[j].doubleValues[i] = passThrough[j].doubleValues[index];
				} else {
					double [] tmp = doubleOutput[i].evaluate(passThrough);
					for (int j=0; j<size; j++)
						output[j].doubleValues[i] = tmp[j];
				}
			}
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
