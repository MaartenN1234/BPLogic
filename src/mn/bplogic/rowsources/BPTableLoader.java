package mn.bplogic.rowsources;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;

import mn.bplogic.main.SqlStatics;
import mn.bplogic.main.TranslateMap;

public class BPTableLoader {
	public static BPTable loadFromTable(String tableName)  throws SqlStatics.SqlStaticsException, SQLException{
		return loadFromQuery(tableName, "SELECT * FROM "+tableName);
	}

	public static BPTable loadFromQuery(String tableName, String query) throws SqlStatics.SqlStaticsException, SQLException{
		Connection        conn   = SqlStatics.getSqlConnection();
		PreparedStatement ps     = conn.prepareStatement(query);
		ResultSet         rs     = ps.executeQuery();
		ResultSetMetaData rsmd   = rs.getMetaData();

		ArrayList<BPRow>   dataPrep         = new ArrayList<BPRow>();
		ArrayList<String>  intLabelsPrep    = new ArrayList<String>();
		ArrayList<Integer> intTypesPrep     = new ArrayList<Integer>();
		ArrayList<String>  doubleLabelsPrep = new ArrayList<String>();

		ArrayList<int[]> readMethodsList = new ArrayList <int[]>();

		// interpret metadata
		for (int i = 1; i<=rsmd.getColumnCount(); i++){
			if (rsmd.getColumnName(i).toUpperCase().equals("STARTDATE")){
				readMethodsList.add(new int[]{BPRowSource.specialSD});
			}else if (rsmd.getColumnName(i).toUpperCase().equals("ENDDATE")){
				readMethodsList.add(new int[]{BPRowSource.specialED});
			}else if (rsmd.getColumnName(i).toUpperCase().equals("MUTNR")){
				readMethodsList.add(new int[]{BPRowSource.specialIgnore});
			} else {
				switch (rsmd.getColumnType(i)) {
				case Types.CHAR:
				case Types.LONGNVARCHAR:
				case Types.LONGVARCHAR:
				case Types.NCHAR:
				case Types.NVARCHAR:
				case Types.VARCHAR:
					// String
					readMethodsList.add(new int[]{BPRowSource.stringType, intLabelsPrep.size()});
					intLabelsPrep.add(rsmd.getColumnName(i));
					intTypesPrep.add(BPRowSource.stringType);
					break;
				case Types.DATE:
				case Types.TIME:
				case Types.TIMESTAMP:
					// Date
					readMethodsList.add(new int[]{BPRowSource.dateType, intLabelsPrep.size()});
					intLabelsPrep.add(rsmd.getColumnName(i));
					intTypesPrep.add(BPRowSource.dateType);
					break;
				case Types.DECIMAL:
				case Types.DOUBLE:
				case Types.FLOAT:
				case Types.REAL:
				case Types.NUMERIC:
					// Double
					readMethodsList.add(new int[]{BPRowSource.doubleType, doubleLabelsPrep.size()});
					doubleLabelsPrep.add(rsmd.getColumnName(i));
					break;
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.TINYINT:
					// Integer
					readMethodsList.add(new int[]{BPRowSource.intType, intLabelsPrep.size()});
					intLabelsPrep.add(rsmd.getColumnName(i));
					intTypesPrep.add(BPRowSource.intType);
					break;
				default:
					// Object
					readMethodsList.add(new int[]{BPRowSource.objectType, intLabelsPrep.size()});
					intLabelsPrep.add(rsmd.getColumnName(i));
					intTypesPrep.add(BPRowSource.objectType);
					break;
				}
			}
		}

		int [][] readMethods = readMethodsList.toArray(new int[0][]);

		TranslateMap<String> tms = TranslateMap.getStringMap();
		TranslateMap<Date>   tmd = TranslateMap.getDateMap();
		TranslateMap<Object> tmo = TranslateMap.getObjectMap();

		// read all rows
		while(rs.next()){
			BPRow bpr = new BPRow(intLabelsPrep.size(), doubleLabelsPrep.size());
			for (int i = 0; i< readMethods.length; i++){
				int k = i+1;
				long l;
				switch(readMethods[i][0]){
				case BPRowSource.specialSD:
					l = rs.getDate(k).getTime()+BPRowSource.halfDayMillis;
					bpr.startIncl = (int) (l / BPRowSource.oneDayMillis);
					break;
				case BPRowSource.specialED:
					l = rs.getDate(k).getTime()+BPRowSource.halfDayMillis;
					bpr.end = 1+(int) (l / BPRowSource.oneDayMillis);
					break;
				case BPRowSource.intType:
					bpr.intValues[readMethods[i][1]] = rs.getInt(k);
					break;
				case BPRowSource.doubleType:
					bpr.doubleValues[readMethods[i][1]] = rs.getDouble(k);
					break;
				case BPRowSource.stringType:
					String s = rs.getString(k);
					bpr.intValues[readMethods[i][1]] = tms.translate(s);
					break;
				case BPRowSource.dateType:
					Date d = rs.getDate(k);
					bpr.intValues[readMethods[i][1]] = tmd.translate(d);
					break;
				case BPRowSource.objectType:
					Object o = rs.getObject(k);
					bpr.intValues[readMethods[i][1]] = tmo.translate(o);
					break;
				}
			}
			dataPrep.add(bpr);
		}

		// close
		rs.close();
		ps.close();

		// transfer data to result
		int [] intTypes = new int[intTypesPrep.size()];
		int i=0;
		for (int j : intTypesPrep){
			intTypes[i++] = j;
		}
		String [] intFieldLabels = intLabelsPrep.toArray(new String[0]);
		String [] doubleFieldLabels = doubleLabelsPrep.toArray(new String[0]);
		BPRow  [] data = dataPrep.toArray(new BPRow[0]);
		return new BPTable(tableName, intTypes, intFieldLabels, doubleFieldLabels, data);
	}
}
