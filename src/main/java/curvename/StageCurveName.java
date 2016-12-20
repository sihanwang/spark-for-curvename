package curvename;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.JdbcRDD;

///local/java/spark/current/bin/spark-submit --class curvename.StageCurveName --master yarn-cluster --deploy-mode cluster lib/MetaDataAutosuggest-0.0.1-SNAPSHOT-jar-with-dependencies.jar 34052 /config/cdb_beta.properties

public class StageCurveName {

	public static void main(String[] args) throws Exception {
		
		System.out.println("###################Started main###################");

		final SparkConf sparkConf = new SparkConf().setAppName("CurveNameStaging");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(sc);
		final String sqlcurvenamerules = "SELECT LONGNAME_RULE_EXP, SHORTNAME_RULE_EXP,DESC_RULE_EXP,CURVENAME_STATUS_ID FROM LOAD_SET2_CURVENAME_RULE WHERE LOAD_SET_ID=?";
		final String sqlgetallcurveattributes = "(select curve_id, dbms_lob.substr(wmsys.WM_CONCAT('\"'||attribute||'.'||verb||'\":{fullname:\"'||element_name||'\",shortname:\"'||element||'\",abbr:\"'||nvl(abbr,' ')||'\"}'),4000) attr from meta.object_meta_v omv, dw.load_set2_curve lsc, dw.load_set2_curve_dict lscd where omv.object_id = lsc.curve_id and omv.element_id = lscd.element_id(+) and lsc.load_set_id = %s group by lsc.curve_id) curve_attr";
		final String InsertIntoStagingTable = "insert into DW.LOAD_SET2_CURVE_NAME_STAGING (CURVE_ID,CURVE_NAME,SHORT_CURVE_NAME,curve_description,LOAD_SET_ID,IS_VALID) values (?,?,?,?,?,?)";
		final String UpdateJobStatus = "update LOAD_SET2_CURVENAME_RULE set CURVENAME_STATUS_ID=? where CURVENAME_STATUS_ID=1 and LOAD_SET_ID=?";

		Properties prop = new Properties();
		prop.load(StageCurveName.class.getResourceAsStream(args[1]));
		System.out.println("###################Loaded config###################");

		final Long LoadsetID = Long.parseLong(args[0]);
		final String DB_USERNAME = prop.getProperty("oracle.user");
		final String DB_PWD = prop.getProperty("oracle.password");
		final String DB_CONNECTION_URL = prop.getProperty("oracle.url");

		class CurveNameResult {
			@Override
			public String toString() {
				// TODO Auto-generated method stub
				return "IsValid_LongName:" + IsValid_LongName + "|" + "IsValid_ShortName:" + IsValid_ShortName + "|"+ IsValid_Desc + "|"
						+ "LongName:" + LongName + "|" + "ShortName:" + ShortName + "|"+ "Description:" + Desc + "|";

			}

			boolean IsValid_LongName;
			boolean IsValid_ShortName;
			boolean IsValid_Desc;

			String LongName, ShortName,Desc;

			CurveNameResult(boolean isvalid_longname, boolean isvalid_shortname,boolean isvalid_desc, String longname, String shortname,String desc) {
				this.IsValid_LongName = isvalid_longname;
				this.IsValid_ShortName = isvalid_shortname;
				this.IsValid_Desc = isvalid_desc;
				this.LongName = longname;
				this.ShortName = shortname;
				this.Desc = desc;
			}
		}
		;

		class CurvenameGeneratorByPartitions
				implements PairFlatMapFunction<java.util.Iterator<Row>, Long, CurveNameResult> {

			String LongRule, ShortRule, DescRule;

			CurvenameGeneratorByPartitions(String lr, String sr, String dr) {

				this.LongRule = lr;
				this.ShortRule = sr;
				this.DescRule = dr;
			}

			@Override
			public Iterable<Tuple2<Long, CurveNameResult>> call(Iterator<Row> rowsinpartition) throws Exception {
				// TODO Auto-generated method stub
				ScriptEngine engine = new NashornScriptEngineFactory()
						.getScriptEngine(new String[] { "-strict", "--no-java", "--no-syntax-extensions" });
				Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

				bindings.remove("print");
				bindings.remove("load");
				bindings.remove("loadWithNewGlobal");
				bindings.remove("exit");
				bindings.remove("quit");

				ArrayList<Tuple2<Long, CurveNameResult>> Tuple2List = new ArrayList<Tuple2<Long, CurveNameResult>>();

				while (rowsinpartition.hasNext()) {
					String jsStrConstant = "\"use strict\";" + "function getAttr(curve) {";
					Row t = (Row) rowsinpartition.next();
					Long curveid = t.getDecimal(0).longValue();
					String AttrJS = (String) t.getString(1);
                    String newAttr = AttrJS;
					String LongName;
					String ShortName;
					String Desc;

					boolean IsValid_LongName = true;
					boolean IsValid_ShortName = true;
					boolean IsValid_Desc = true;

					try {

						String[] attrArr = AttrJS.split("},");
						Hashtable<String,Integer> attrHt = new Hashtable<String,Integer>();
						for (String meta : attrArr) {
							String[] temp = meta.split("\\{");
//							jsStrConstant = jsStrConstant + " var "
//									+ temp[0].replace(".", "").replace("\"", "").replace(":", "") + " = curve["
//									+ temp[0].replace(":", "") + "]; ";
							if(attrHt.get(temp[0].replace(".", "").replace("\"", "").replace(":", ""))==null){
								jsStrConstant = jsStrConstant+" var "+temp[0].replace(".", "").replace("\"", "").replace(":", "") + " = curve["+temp[0].replace(":", "")+"]; ";
                                attrHt.put(temp[0].replace(".", "").replace("\"", "").replace(":", ""),1 );
                           }else{
                         	    int count=attrHt.get(temp[0].replace(".", "").replace("\"", "").replace(":", ""));
                         	    jsStrConstant = jsStrConstant+" var "+temp[0].replace(".", "").replace("\"", "").replace(":", "") + count+" = curve[\""+temp[0].replace(":", "").replace("\"", "")+ count+"\"]; ";
                         	   attrHt.put(temp[0].replace(".", "").replace("\"", "").replace(":", ""),count+1 );
                         	  int pos = getCharacterPosition(AttrJS,count+1,temp[0]);
                       	       newAttr = AttrJS.substring(0, pos)+"\""+temp[0].replace("\"", "").replace(":", "")+count+"\":"+AttrJS.substring(pos+temp[0].length());
                       	          
                           }
						}

						// long curve name generation
						String jsforname = jsStrConstant + this.LongRule + "}; getAttr({" + newAttr + "});";

						try {
							LongName = (String) engine.eval(jsforname);
							
							if (LongName.length()>64)
							{
								LongName = "Failed to generate long curve name for the curve:"+ curveid+" because its result name "+ LongName+" is longer than 64 characters";
								IsValid_LongName=false;
							}
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							System.out.println(jsforname);
							LongName = "Failed to generate long curve name for the curve:" + curveid
									+ " because of exception:" + e.getMessage();
							IsValid_LongName = false;
						}

						// Short curve name generation
						jsforname = jsStrConstant + this.ShortRule + "}; getAttr({" + newAttr + "});";

						try {
							ShortName = (String) engine.eval(jsforname);
							
							if (ShortName.length()>32)
							{
								ShortName = "Failed to generate short curve name for the curve:"+ curveid+" because its result name "+ ShortName+" is longer than 32 characters";
								IsValid_ShortName=false;
							}			
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							System.out.println(jsforname);
							ShortName = "Failed to generate short curve name for the curve:" + curveid
									+ " because of exception:" + e.getMessage();
							IsValid_ShortName = false;
						}
						
						// curve description generation
						 jsforname = jsStrConstant + this.DescRule + "}; getAttr({" + newAttr + "});";
						 System.out.println("##############  Desc rule  ################ "+this.DescRule);
						try {
							Desc = (String) engine.eval(jsforname);
							
						} catch (Exception e) {
							// TODO Auto-generated catch block
							System.out.println(jsforname);
							Desc = "Failed to generate description for the curve:" + curveid
									+ " because of exception:" + e.getMessage();
							 
						}
						System.out.println("##############  Desc  ################ "+Desc);

					} catch (Exception e) {
						// TODO Auto-generated catch block
						IsValid_LongName = false;
						IsValid_ShortName = false;
						LongName = "Failed to generate curve name for the curve:" + curveid + " because of exception:"
								+ e.getMessage();
						ShortName = "Failed to generate curve name for the curve:" + curveid + " because of exception:"
								+ e.getMessage();
						Desc = "Failed to generate descripton for the curve:" + curveid + " because of exception:"
								+ e.getMessage();
					}

					if (LongName.length() > 4000) {
						LongName = LongName.substring(0, 4000);
					}

					if (ShortName.length() > 4000) {
						ShortName = ShortName.substring(0, 4000);
					}
					
					if (Desc.length() > 4000) {
						Desc = Desc.substring(0, 4000);
					}

					Tuple2List.add(new Tuple2<Long, CurveNameResult>(curveid,
							new CurveNameResult(IsValid_LongName, IsValid_ShortName,IsValid_Desc, LongName, ShortName,Desc)));

				}

				return Tuple2List;

			}
			
		 
		    public   int getCharacterPosition(String string ,int i,String character){  
		       
		       // Matcher slashMatcher = Pattern.compile("/").matcher(string);  
		         Matcher slashMatcher = Pattern.compile(character).matcher(string);  
		        int mIdx = 0;  
		        while(slashMatcher.find()) {  
		           mIdx++;  
		           
		           if(mIdx == i){  
		              break;  
		           }  
		        }  
		        return slashMatcher.start();  
		     }  

		}
		;

		class DbConnection implements JdbcRDD.ConnectionFactory {

			private String driverClassName;
			private String connectionUrl;
			private String userName;
			private String password;

			public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
				this.driverClassName = driverClassName;
				this.connectionUrl = connectionUrl;
				this.userName = userName;
				this.password = password;
			}

			@Override
			public Connection getConnection() throws Exception {
				// TODO Auto-generated method stub
				Connection connection = null;

				Class.forName(driverClassName);
				Properties properties = new Properties();
				properties.setProperty("user", userName);
				properties.setProperty("password", password);

				connection = DriverManager.getConnection(connectionUrl, properties);

				return connection;
			}
		}
		;
		
		DbConnection ConnFactory = new DbConnection("oracle.jdbc.driver.OracleDriver", DB_CONNECTION_URL,	DB_USERNAME, DB_PWD);
		Connection con = null;
		PreparedStatement pre = null;
		ResultSet result = null;
				
		try {
			
			// Check Curvename status in load_set2_curvename_rule
			con = ConnFactory.getConnection();
			
			System.out.println("###################Got DB connection###################");
			
			con.setAutoCommit(false);

			String long_rule = "";
			String short_rule = "";
			String desc_rule = "";
			Integer curvename_status = -2;

			// Query curve name rules
			try {
				
				System.out.println("###################About to run sqlcurvenamerules###################");
				// Search out curve name rules from database
				pre = con.prepareStatement(sqlcurvenamerules);
				pre.setLong(1, LoadsetID);
				System.out.println("###################About to query sqlcurvenamerules###################");
				result = pre.executeQuery();
				System.out.println("###################Executed query sqlcurvenamerules###################");
				if (result.next()) {
					
					System.out.println("###################Got at least one result###################");

					curvename_status = result.getInt("CURVENAME_STATUS_ID");

					char[] buffer = new char[1024];

//					Reader reader_long_rule = result.getCharacterStream("LONGNAME_RULE_EXP");
//
//					int i = 0;
//
//					while ((i = reader_long_rule.read(buffer)) != -1) {
//						long_rule = long_rule.concat(new String(buffer));
//					}
//
//					reader_long_rule.close();
//
//					Reader reader_short_rule = result.getCharacterStream("SHORTNAME_RULE_EXP");
//
//					i = 0;
//
//					while ((i = reader_short_rule.read(buffer)) != -1) {
//						short_rule = short_rule.concat(new String(buffer));
//					}
//
//					reader_short_rule.close();
					Clob long_rule_clob = result.getClob("LONGNAME_RULE_EXP") ;
					Clob short_rule_clob = result.getClob("SHORTNAME_RULE_EXP");
					Clob desc_rule_clob = result.getClob("DESC_RULE_EXP");
					long_rule = long_rule_clob.getSubString(1, (int) long_rule_clob.length());
					short_rule = short_rule_clob.getSubString(1, (int) short_rule_clob.length());
					desc_rule = desc_rule_clob.getSubString(1, (int) desc_rule_clob.length());
					System.out.println("long_rule = "+long_rule);
					System.out.println("short_rule = "+short_rule);
					System.out.println("desc_rule = "+desc_rule);

					curvename_status = result.getInt("CURVENAME_STATUS_ID");

					// Verifying if status, rule values have been correctly
					// populated.

					if (curvename_status != 1) {
						//throw new Exception("Loadset:" + LoadsetID + " is not in staging status!");
					}

					if (long_rule.equals("")) {
						throw new Exception("Loadset:" + LoadsetID + " doesn't have rule for long curve name");
					}

					if (short_rule.equals("")) {
						throw new Exception("Loadset:" + LoadsetID + " doesn't have rule for short curve name");
					}
					
					if (desc_rule.equals("")) {
						throw new Exception("Loadset:" + LoadsetID + " doesn't have rule for curve description");
					}

				} else {
					throw new Exception("Load set:" + LoadsetID + " doesn't exist!");
				}
				
				System.out.println("###################Fetched rules###################");

				// To do:clear up previously staged curve names
				pre.close();
				result.close();

				pre = con.prepareStatement("delete from DW.LOAD_SET2_CURVE_NAME_STAGING where load_set_id=?");
				pre.setLong(1, LoadsetID);
				pre.executeUpdate();

				pre.close();
				con.commit();

			} finally {
				if (con != null) {
					con.close();
				}
			}
			
			System.out.println("###################Retrived rules###################");

			Map<String, String> options = new HashMap<String, String>();
			options.put("driver", "oracle.jdbc.driver.OracleDriver");
			options.put("url", DB_CONNECTION_URL);
			options.put("dbtable", String.format(sqlgetallcurveattributes, LoadsetID));
			options.put("partitionColumn", "curve_id");
			options.put("lowerBound", "1");
			options.put("upperBound", String.valueOf(Integer.MAX_VALUE));
			options.put("numPartitions", "1");
			options.put("fetchSize", "1000");
			options.put("user", DB_USERNAME);
			options.put("password", DB_PWD);
			// Load query result as DataFrame
			DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();

			JavaRDD<Row> CurveIDList = jdbcDF.javaRDD();

			// CurvenameGenerator thisGenerator = new
			// CurvenameGenerator(long_rule, short_rule);
			CurvenameGeneratorByPartitions thisGenerator = new CurvenameGeneratorByPartitions(long_rule, short_rule,desc_rule);

			JavaPairRDD<Long, CurveNameResult> CurveID_Name_List = CurveIDList.mapPartitionsToPair(thisGenerator);

			CurveID_Name_List.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY());

			System.out.println("###################Start CurveID_Name_List###################");
			CurveID_Name_List.foreach(new VoidFunction<Tuple2<Long, CurveNameResult>>() {

				@Override
				public void call(Tuple2<Long, CurveNameResult> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("CurveID_Name_List#" + t._1 + ":" + t._2.toString());
				}
			});
			System.out.println("###################End CurveID_Name_List###################");

			// Mark duplication
			// Filter Valid long name
			class FilterValidName implements Function<Tuple2<Long, CurveNameResult>, Boolean> {
				boolean FilterLong = true;

				FilterValidName(boolean filterlong) {
					this.FilterLong = filterlong;
				}

				@Override
				public Boolean call(Tuple2<Long, CurveNameResult> v1) throws Exception {
					// TODO Auto-generated method stub

					if (this.FilterLong) {
						return v1._2.IsValid_LongName ? true : false;
					} else {
						return v1._2.IsValid_ShortName ? true : false;
					}
				}

			}
			;

			class GetOneOfName implements Function<CurveNameResult, String> {

				boolean LongName = true;

				GetOneOfName(boolean longname) {
					this.LongName = longname;
				}

				@Override
				public String call(CurveNameResult v1) throws Exception {
					// TODO Auto-generated method stub

					if (this.LongName) {
						return v1.LongName;
					} else {
						return v1.ShortName;
					}
				}
			}
			;

			JavaPairRDD<Long, String> ValidLongName = CurveID_Name_List.filter(new FilterValidName(true))
					.mapValues(new GetOneOfName(true));

			System.out.println("###################Start ValidLongName###################");
			ValidLongName.foreach(new VoidFunction<Tuple2<Long, String>>() {

				@Override
				public void call(Tuple2<Long, String> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("ValidLongName#" + t._1 + ":" + t._2);
				}
			});
			System.out.println("###################End ValidLongName###################");

			class GroupByName implements Function<Tuple2<Long, String>, String> {
				@Override
				public String call(Tuple2<Long, String> v1) throws Exception {
					// TODO Auto-generated method stub
					return v1._2;
				}

			}
			;

			class FlatDupName
					implements PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<Long, String>>>, Long, String> {
				boolean ForLong = true;

				FlatDupName(boolean forlong) {
					this.ForLong = forlong;
				}

				@Override
				public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Tuple2<Long, String>>> t)
						throws Exception {
					// TODO Auto-generated method stub

					ArrayList<Tuple2<Long, String>> Tuple2List = new ArrayList<Tuple2<Long, String>>();

					Iterator<Tuple2<Long, String>> It = t._2.iterator();

					boolean IsTheFirst = true;
					Long TheFirstCurveID = null;

					while (It.hasNext()) {
						Tuple2<Long, String> thisTuple = It.next();

						if (!IsTheFirst) {
							if (ForLong) {
								Tuple2List
										.add(new Tuple2<Long, String>(thisTuple._1,
												"Failed to generate long curve name for the curve:" + thisTuple._1
														+ " because its result name is same as curve:"
														+ TheFirstCurveID));
							} else {
								Tuple2List
										.add(new Tuple2<Long, String>(thisTuple._1,
												"Failed to generate short curve name for the curve:" + thisTuple._1
														+ " because its result name is same as curve:"
														+ TheFirstCurveID));
							}
						} else {
							IsTheFirst = false;
							TheFirstCurveID = thisTuple._1;
						}

					}
					return Tuple2List;
				}
			}

			JavaPairRDD<Long, String> DupLongName = ValidLongName.groupBy(new GroupByName())
					.flatMapToPair(new FlatDupName(true));

			System.out.println("###################Start DupLongName###################");
			DupLongName.foreach(new VoidFunction<Tuple2<Long, String>>() {

				@Override
				public void call(Tuple2<Long, String> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("DupLongName#" + t._1 + ":" + t._2);
				}
			});
			System.out.println("###################End DupLongName###################");

			// Filter Valid short name
			JavaPairRDD<Long, String> ValidShortName = CurveID_Name_List.filter(new FilterValidName(false))
					.mapValues(new GetOneOfName(false));

			System.out.println("###################Start ValidShortName###################");
			ValidShortName.foreach(new VoidFunction<Tuple2<Long, String>>() {

				@Override
				public void call(Tuple2<Long, String> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("ValidShortName#" + t._1 + ":" + t._2);
				}
			});
			System.out.println("###################End ValidShortName###################");

			JavaPairRDD<Long, String> DupShortName = ValidShortName.groupBy(new GroupByName())
					.flatMapToPair(new FlatDupName(false));
			System.out.println("###################Start DupLongName###################");
			DupShortName.foreach(new VoidFunction<Tuple2<Long, String>>() {

				@Override
				public void call(Tuple2<Long, String> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("DupShortName#" + t._1 + ":" + t._2);
				}
			});
			System.out.println("###################End DupShortName###################");

			class CombineNameWithDup implements
					PairFunction<Tuple2<Long, Tuple2<CurveNameResult, com.google.common.base.Optional<String>>>, Long, CurveNameResult> {
				boolean ForLong = true;

				CombineNameWithDup(boolean forlong) {
					this.ForLong = forlong;
				}

				@Override
				public Tuple2<Long, CurveNameResult> call(
						Tuple2<Long, Tuple2<CurveNameResult, com.google.common.base.Optional<String>>> t)
						throws Exception {
					// TODO Auto-generated method stub

					Long curveid = t._1;
					CurveNameResult thisCNR = t._2._1;
					com.google.common.base.Optional<String> dupName = t._2._2;

					if (dupName.isPresent()) {
						if (ForLong) {
							thisCNR.IsValid_LongName = false;
							thisCNR.LongName = dupName.get();
						} else {
							thisCNR.IsValid_ShortName = false;
							thisCNR.ShortName = dupName.get();
						}
					}
					return new Tuple2<Long, CurveNameResult>(curveid, thisCNR);

				}

			}

			CurveID_Name_List = CurveID_Name_List.leftOuterJoin(DupLongName).mapToPair(new CombineNameWithDup(true))
					.leftOuterJoin(DupShortName).mapToPair(new CombineNameWithDup(false));

			System.out.println("###################Start CurveID_Name_List###################");
			CurveID_Name_List.foreach(new VoidFunction<Tuple2<Long, CurveNameResult>>() {

				@Override
				public void call(Tuple2<Long, CurveNameResult> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println("CurveID_Name_List#" + t._1 + ":" + t._2.toString());
				}
			});
			System.out.println("###################End CurveID_Name_List###################");

			class SaveToStagingTable implements VoidFunction<Iterator<Tuple2<Long, CurveNameResult>>> {

				JdbcRDD.ConnectionFactory dbconnfactory;

				SaveToStagingTable(JdbcRDD.ConnectionFactory cnfactory) {
					dbconnfactory = cnfactory;
				}

				@Override
				public void call(Iterator<Tuple2<Long, CurveNameResult>> itemsinpartition) throws Exception {
					// TODO Auto-generated method stub
					Connection dbconn = dbconnfactory.getConnection();

					String curvename = null;
					String shortname = null;
					String desc = null;
					String isvalid = null;

					try {
						dbconn.setAutoCommit(false);

						while (itemsinpartition.hasNext()) {
							Tuple2<Long, CurveNameResult> t = (Tuple2<Long, CurveNameResult>) itemsinpartition.next();

							Long curveid = t._1;
							curvename = t._2.LongName;
							shortname = t._2.ShortName;
							isvalid = (t._2.IsValid_LongName && t._2.IsValid_ShortName) ? "Y" : "N";
							desc = t._2.Desc;
							PreparedStatement psInsertIntoStaging = dbconn.prepareStatement(InsertIntoStagingTable);
							psInsertIntoStaging.setLong(1, curveid);
							psInsertIntoStaging.setString(2, curvename);
							psInsertIntoStaging.setString(3, shortname);
							psInsertIntoStaging.setString(4, desc);
							psInsertIntoStaging.setLong(5, LoadsetID);
							psInsertIntoStaging.setString(6, isvalid);
							psInsertIntoStaging.executeUpdate();

						}

						dbconn.commit();
					} catch (Exception e) {
						System.out.println("curvename:" + curvename);
						System.out.println("shortname:" + shortname);
						System.out.println("isvalid:" + isvalid);

						e.printStackTrace(System.out);

					} finally {
						// TODO Auto-generated catch block
						if (dbconn != null) {
							dbconn.close();
						}
					}

				}

			}

			SaveToStagingTable STST = new SaveToStagingTable(ConnFactory);

			CurveID_Name_List.foreachPartition(STST);
			
			//Update CURVENAME_STATUS_ID to 2
			con = ConnFactory.getConnection();
			con.setAutoCommit(false);

			try {
				pre = con.prepareStatement(UpdateJobStatus);
				pre.setInt(1, 2);
				pre.setLong(2,LoadsetID);
				pre.executeUpdate();
				con.commit();
				pre.close();
				
			} finally {
				
				if (con!=null)
				{
					con.close();
				}
			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//Update CURVENAME_STATUS_ID to -1
			
			con = ConnFactory.getConnection();
			con.setAutoCommit(false);

			try {
				pre = con.prepareStatement(UpdateJobStatus);
				pre.setInt(1, -1);
				pre.setLong(2,LoadsetID);
				pre.executeUpdate();
				con.commit();
				pre.close();
				
			} finally {
				
				if (con!=null)
				{
					con.close();
				}
			}
			
		}

	}

}
