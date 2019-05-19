package mn.bplogic.main;
import java.util.HashMap;

import mn.bplogic.api.*;
import mn.bplogic.api.expressions.*;
import mn.bplogic.rowsources.*;

public class TestMain {

	public static void testScriptFiltProj() throws Exception{
		TranslateMap<String> tms = TranslateMap.getStringMap();
		BPTable test = BPTableLoader.loadFromTable("BP_IDP_CURRENCY");

		BPRowSource testFilt;

		// Expression API 1506ms (1 mln calls)
		//*
		IntExpression proxy_ccy_id = new IntFetchValue("PROXY_CCY_ID" , test);
		IntExpression ccy_id       = new IntFetchValue("CCY_ID" , test);
		DoubleExpression hrm       = new DoubleFetchValue("HEDGE_RATIO_MARKET", test);
		BooleanExpression filter   = new Compare(proxy_ccy_id, ccy_id, Compare.UNEQUAL);


		HashMap<String, NumberExpression> outputColumns = new HashMap<String, NumberExpression>();
		outputColumns.put("CONCAT_VALUE", 			new StringConcat(proxy_ccy_id, ccy_id));
		outputColumns.put("PROXY_CCY_ID", 			proxy_ccy_id);
		outputColumns.put("CCY_ID", 				ccy_id);
		outputColumns.put("HEDGE_RATIO_MARKET", 	hrm);

		testFilt = new BPFilterAndProjectRowSource("filter: PROXY_CCY!=CCY, project", test,
				new ExpressionFilterAndProjector(outputColumns, filter));

		// */
		//*
		long s = System.currentTimeMillis();
		for(int i =0; i< 1000000; i++){
			testFilt.getIterator().nextBuffer(100);
		}
		System.out.println("Timing: "+(System.currentTimeMillis()-s)+"ms");
		// */


		BPRowIterator bpIter;
		bpIter = test.getPredicatedIterator(new String[]{"PROXY_CCY_ID"}, new int[]{tms.translate("JPY")});
		System.out.println(testFilt);
		bpIter = testFilt.getSortedIterator(new String[]{"CONCAT_VALUE"});
		bpIter = testFilt.getPredicatedIterator(new String[]{"PROXY_CCY_ID"}, new int[]{tms.translate("Jpy")});

		BPRow bpr = bpIter.next();
		while(bpr != null){
			System.out.println(bpr);
			bpr = bpIter.next();
		}

		test = BPTableLoader.loadFromTable("BP_IDP_CURRENCY");
		System.gc();

		System.out.println(test);

	}
	public static void testAggreg() throws Exception{
		BPTable test = BPTableLoader.loadFromTable("BP_IDP_CURRENCY");

		BPRowSource testFilt;
		BPRowSource testFilt2;
		IntExpression idp          = new IntFetchValue("IDP" , test);
		IntExpression ccy_id       = new IntFetchValue("CCY_ID" , test);
		DoubleExpression hrm       = new DoubleFetchValue("HEDGE_RATIO_MARKET", test);

		HashMap<String, NumberExpression> outputColumns = new HashMap<String, NumberExpression>();
		outputColumns.put("IDP", 			idp);
		outputColumns.put("COUNT", 			new IntGroupCount());
		outputColumns.put("CCY_F", 			new IntGroupMin(ccy_id));
		outputColumns.put("CCY_L", 			new IntGroupMax(ccy_id));
		outputColumns.put("hrmSum", 		new DoubleGroupSum(hrm));


		testFilt = new BPAggregateRowSource("Group by test", test, new String[]{"IDP"}, new ExpressionAggregator(outputColumns));
		// 8172ms for 1 mln fetches
		///*
		long s = System.currentTimeMillis();
		for(int i =0; i< 1000000; i++){
			@SuppressWarnings("unused")
			BPRow [] bpr = testFilt.getIterator().nextBuffer(100);
		}
		System.out.println("Timing: "+(System.currentTimeMillis()-s)+"ms");
		// */
		testFilt2 = new BPAggregateRowSource("Priority", testFilt, new String[]{"CCY_L"}, new PriorityExpressionAggregator(testFilt, "hrmSum", true));


		System.out.println(testFilt);
		System.out.println(test);
		System.out.println(testFilt2);

	}

	public static void testJoin() throws Exception{
		BPTable test1 = BPTableLoader.loadFromTable("BP_IDP_CURRENCY");
		BPTable test2 = BPTableLoader.loadFromTable("BP_CURRENCY");

		BPRowSource testFilt;

		testFilt = new BPMergeJoinRowSource("Merge join test", test1, test2, new String[]{"CCY_ID"}, new String[]{"CCY_ID"}, BPMergeJoinRowSource.LEFT_OUTER_JOIN);
		System.out.println(test2);
		System.out.println(testFilt);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			//testScriptFiltProj();
			//testAggreg();
			testJoin();
			System.out.println(MemoryStatics.getTechnicalFootprint());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
