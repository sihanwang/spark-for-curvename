package autosuggest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Optional;

import curvename.StageCurveName;
import scala.Tuple2;

//spark-submit --class autosuggest.ExactCurveMeta --master yarn-cluster --deploy-mode cluster --queue commoditiesdata MetaDataAutosuggest-0.0.1-SNAPSHOT.jar 36251 /config/autosuggest_alpha.properties output 1

public class ExactCurveMeta {
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


	public static void main(String[] args) throws Exception {

		class CurveMetaData {

			private String attr;
			private String verb;
			private String name;
			private String rcscode;

			public CurveMetaData(String attr, String verb, String name,String rcscode) {
				super();
				this.attr = attr;
				this.verb = verb;
				this.name = name;
				this.rcscode=rcscode;
			}

			public String getAttr() {
				return attr;
			}

			public String getVerb() {
				return verb;
			}

			public String getName() {
				return name;
			}

			
			public String getrcscode() {
				return rcscode;
			}			

		};

		class ConvertToPairRDDForMetaData implements PairFunction<Row, Long, CurveMetaData> {

			@Override
			public Tuple2<Long, CurveMetaData> call(Row t) throws Exception {
				// TODO Auto-generated method stub

				Long curveid = t.getDecimal(0).longValue();
				String attr =  (t.isNullAt(2) ? "":t.get(2).toString());
				String verb =  (t.isNullAt(3) ? "":t.get(3).toString());
				String name =  (t.isNullAt(5) ? "":t.get(5).toString());
				String ric =  (t.isNullAt(4) ? "":t.get(4).toString());

				return new Tuple2<Long, CurveMetaData>(curveid, new CurveMetaData(attr, verb, name,ric));

			}

		};
		

		class CurveNames {

			public CurveNames(String curvename, String shortcurvename, String description) {
				super();
				this.curvename = curvename;
				this.shortcurvename = shortcurvename;
				this.description=description;
			}

			private String curvename;
			private String shortcurvename;
			private String description;

			public String getCurvename() {
				return curvename;
			}

			public String getshortcurvename() {
				return shortcurvename;
			}
			
			public String getdescription() {
				return description;
			}			
		};		
		
		
				
		class GenerateSDI implements
		PairFunction<Tuple2<Long, Tuple2<Iterable<CurveMetaData>, com.google.common.base.Optional<CurveNames>>>, Long, String> {

			@Override
			public Tuple2<Long, String> call(
					Tuple2<Long, Tuple2<Iterable<CurveMetaData>, com.google.common.base.Optional<CurveNames>>> t)
							throws Exception {
				// TODO Auto-generated method stub

				Long curveid = t._1;
				Iterable<CurveMetaData> itaMetaData = t._2._1;
				com.google.common.base.Optional<CurveNames> OptionalCurveNames = t._2._2;

				String XMLElement = "<DOC id=" + curveid + " TYPE=\"CDBTIMESERIES\"><Permission>"+curveid+"</Permission>";

				if (OptionalCurveNames.isPresent()) {
					String NameElements = "<name>" + OptionalCurveNames.get().getCurvename() + "</name>";
					NameElements = NameElements + "<shortname>" + OptionalCurveNames.get().getshortcurvename()
							+ "</shortname>";
					NameElements = NameElements +"<description>" + OptionalCurveNames.get().getdescription()
							+ "</description>";
					XMLElement = XMLElement + NameElements;
				}

				Iterator<CurveMetaData> itr = itaMetaData.iterator();

				String CMDElement = "";
				String GeographyElement = "";
				String GeographyRCSElement = "";
				String PhysicalAssetElement = "";
				String PhysicalAssetRCSElement = "";
				
				

				while (itr.hasNext()) {
					CurveMetaData CMD = itr.next();
					
					if (CMD.attr.equals("Provider") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Provider>"+CMD.name+"</Provider>";
					}
					else if (CMD.attr.equals("Timezone") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Timezone>"+CMD.name+"</Timezone>";
					}
					else if (CMD.attr.equals("Value.Frequency") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<ValueFrequency>"+CMD.name+"</ValueFrequency>";
					}
					else if (CMD.attr.equals("Status") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Status>"+CMD.name+"</Status>";
					}
					else if (CMD.attr.equals("Unit") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Unit>"+CMD.name+"</Unit>";
					}
					else if (CMD.attr.equals("Issue.Frequency") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<IssueFrequency>"+CMD.name+"</IssueFrequency>";
					}
					else if (CMD.attr.equals("Variable") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Variable>"+CMD.name+"</Variable>";
					}		
					else if (CMD.attr.equals("Commodity") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Commodity>"+CMD.name+"</Commodity>";
					}			
					else if (CMD.attr.equals("Technology") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<Technology>"+CMD.name+"</Technology>";
					}
					else if (CMD.attr.equals("FacilityType") && CMD.verb.equals("is") && CMD.name.length()>0 )
					{
						CMDElement=CMDElement+"<PhysicalAssetType>"+CMD.name+"</PhysicalAssetType>";
					}									
					else if (CMD.attr.equals("Variable.Specification") && CMD.verb.equals("is") && CMD.name.length()>0)
					{
						CMDElement=CMDElement+"<VariableSpecification>"+CMD.name+"</VariableSpecification>";
					}						
					else if (CMD.attr.equals("Geography") && CMD.name.length()>0 )
					{
						GeographyElement=GeographyElement+CMD.name+"|";
						GeographyRCSElement=GeographyRCSElement+CMD.rcscode+"|";
					}
					else if (CMD.attr.equals("Facility") && CMD.name.length()>0)
					{
						PhysicalAssetElement=PhysicalAssetElement+CMD.name+"|";
						PhysicalAssetRCSElement=PhysicalAssetRCSElement+CMD.rcscode+"|";
					}
			
				}
				
				XMLElement=XMLElement+CMDElement;
				
				if (GeographyElement.length()>0)
				{
					GeographyElement="<Geography>"+GeographyElement.substring(0, GeographyElement.length()-1)+"</Geography>";
					
					if (GeographyRCSElement.length()>0)
					{
						GeographyRCSElement="<GeographyRCS>"+GeographyRCSElement.substring(0, GeographyRCSElement.length()-1)+"</GeographyRCS>";
					}
					
					
					XMLElement=XMLElement+GeographyElement+GeographyRCSElement;
				}
				
				if (PhysicalAssetElement.length()>0)
				{
					PhysicalAssetElement="<PhysicalAsset>"  +PhysicalAssetElement.substring(0, PhysicalAssetElement.length()-1)+"</PhysicalAsset>";
					
					if (PhysicalAssetRCSElement.length()>0)
					{
						PhysicalAssetRCSElement="<PhysicalAssetRCS>"+ PhysicalAssetRCSElement.substring(0, PhysicalAssetRCSElement.length()-1)+"</PhysicalAssetRCS>";
					}
				}
				
				XMLElement=XMLElement+PhysicalAssetElement+PhysicalAssetRCSElement+"</DOC>";
				

				return new Tuple2<Long, String>(curveid, XMLElement);

			}

		};

		class GenerateSDI1 implements FlatMapFunction<Iterator<Tuple2<Long, Tuple2<Iterable<CurveMetaData>, com.google.common.base.Optional<CurveNames>>>>, String> {

			@Override
			public Iterable<String> call(
					Iterator<Tuple2<Long, Tuple2<Iterable<CurveMetaData>, Optional<CurveNames>>>> CurvesInPartition) throws Exception {
				// TODO Auto-generated method stub
				LinkedList<String> CurveXMLList=new LinkedList<String>();
				
				while(CurvesInPartition.hasNext())
				{
					Tuple2<Long, Tuple2<Iterable<CurveMetaData>, Optional<CurveNames>>> t=CurvesInPartition.next();
					Long curveid = t._1;
					Iterable<CurveMetaData> itaMetaData = t._2._1;
					com.google.common.base.Optional<CurveNames> OptionalCurveNames = t._2._2;

					String XMLElement = "<DOC id=" + curveid + " TYPE=\"CDBTIMESERIES\"><Permission>"+curveid+"</Permission>";

					if (OptionalCurveNames.isPresent()) {
						String NameElements = "<name>" + OptionalCurveNames.get().getCurvename() + "</name>";
						NameElements = NameElements + "<shortname>" + OptionalCurveNames.get().getshortcurvename()
								+ "</shortname>";
						NameElements = NameElements +"<description>" + OptionalCurveNames.get().getdescription()
								+ "</description>";
						XMLElement = XMLElement + NameElements;
					}

					Iterator<CurveMetaData> itr = itaMetaData.iterator();

					String CMDElement = "";
					String GeographyElement = "";
					String GeographyRCSElement = "";
					String PhysicalAssetElement = "";
					String PhysicalAssetRCSElement = "";
					
					

					while (itr.hasNext()) {
						CurveMetaData CMD = itr.next();
						
						if (CMD.attr.equals("Provider") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Provider>"+CMD.name+"</Provider>";
						}
						else if (CMD.attr.equals("Timezone") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Timezone>"+CMD.name+"</Timezone>";
						}
						else if (CMD.attr.equals("Value.Frequency") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<ValueFrequency>"+CMD.name+"</ValueFrequency>";
						}
						else if (CMD.attr.equals("Status") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Status>"+CMD.name+"</Status>";
						}
						else if (CMD.attr.equals("Unit") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Unit>"+CMD.name+"</Unit>";
						}
						else if (CMD.attr.equals("Issue.Frequency") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<IssueFrequency>"+CMD.name+"</IssueFrequency>";
						}
						else if (CMD.attr.equals("Variable") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Variable>"+CMD.name+"</Variable>";
						}		
						else if (CMD.attr.equals("Commodity") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Commodity>"+CMD.name+"</Commodity>";
						}			
						else if (CMD.attr.equals("Technology") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<Technology>"+CMD.name+"</Technology>";
						}
						else if (CMD.attr.equals("FacilityType") && CMD.verb.equals("is") && CMD.name.length()>0 )
						{
							CMDElement=CMDElement+"<PhysicalAssetType>"+CMD.name+"</PhysicalAssetType>";
						}									
						else if (CMD.attr.equals("Variable.Specification") && CMD.verb.equals("is") && CMD.name.length()>0)
						{
							CMDElement=CMDElement+"<VariableSpecification>"+CMD.name+"</VariableSpecification>";
						}						
						else if (CMD.attr.equals("Geography") && CMD.name.length()>0 )
						{
							GeographyElement=GeographyElement+CMD.name+"|";
							GeographyRCSElement=GeographyRCSElement+CMD.rcscode+"|";
						}
						else if (CMD.attr.equals("Facility") && CMD.name.length()>0)
						{
							PhysicalAssetElement=PhysicalAssetElement+CMD.name+"|";
							PhysicalAssetRCSElement=PhysicalAssetRCSElement+CMD.rcscode+"|";
						}
				
					}
					
					XMLElement=XMLElement+CMDElement;
					
					if (GeographyElement.length()>0)
					{
						GeographyElement="<Geography>"+GeographyElement.substring(0, GeographyElement.length()-1)+"</Geography>";
						
						if (GeographyRCSElement.length()>0)
						{
							GeographyRCSElement="<GeographyRCS>"+GeographyRCSElement.substring(0, GeographyRCSElement.length()-1)+"</GeographyRCS>";
						}
						
						
						XMLElement=XMLElement+GeographyElement+GeographyRCSElement;
					}
					
					if (PhysicalAssetElement.length()>0)
					{
						PhysicalAssetElement="<PhysicalAsset>"  +PhysicalAssetElement.substring(0, PhysicalAssetElement.length()-1)+"</PhysicalAsset>";
						
						if (PhysicalAssetRCSElement.length()>0)
						{
							PhysicalAssetRCSElement="<PhysicalAssetRCS>"+ PhysicalAssetRCSElement.substring(0, PhysicalAssetRCSElement.length()-1)+"</PhysicalAssetRCS>";
						}
					}
					
					XMLElement=XMLElement+PhysicalAssetElement+PhysicalAssetRCSElement+"</DOC>";		
					CurveXMLList.add(XMLElement);					
				}
				

				if (CurveXMLList.size()>0)
				{
					CurveXMLList.addFirst("<DOCS>");
					CurveXMLList.addLast("</DOCS>");;
				}
								
				return CurveXMLList;
			}

		};		

		class ConvertToWritableTypes implements PairFunction<Tuple2<Long, String>, LongWritable, Text> {
			public Tuple2<LongWritable, Text> call(Tuple2<Long, String> record) {
				return new Tuple2(new LongWritable(record._1), new Text(record._2));
			}
		};

		class ConvertToPairRDDForNames implements PairFunction<Row, Long, CurveNames> {

			@Override
			public Tuple2<Long, CurveNames> call(Row t) throws Exception {
				// TODO Auto-generated method stub

				Long curveid = t.getDecimal(0).longValue();
				String curvename = (String) t.getString(2);
				String shortcurvename = (String) t.getString(3);
				String curvedescription= (String) t.getString(4);

				return new Tuple2<Long, CurveNames>(curveid, new CurveNames(curvename, shortcurvename,curvedescription));

			}

		}
		;

		final SparkConf sparkConf = new SparkConf().setAppName("ExactCurveMeta");
		
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		final SQLContext sqlContext = new SQLContext(sc);
		final String sqlgetcurveattributesbyloadset = "(SELECT om.object_id curve_id,mod(om.object_id,1000) partition_id,attribute,verb,ric,element_name name FROM meta.object_meta_v om WHERE om.object_type_id = 1 AND om.object_id IN (select curve_id from dw.load_set2_curve where load_set_id=%s)) curve_attr";
		final String sqlgetallcurveattributes = "(SELECT om.object_id curve_id,mod(om.object_id,1000) partition_id,attribute,verb,ric,element_name name FROM meta.object_meta_v om WHERE om.object_type_id = 1 ) curve_attr";
		final String sqlgetcurvenamesbyloadset = "(select curve_id, mod(curve_id,1000) partition_id, curve_name, short_curve_name,curve_description from load_set2_curve_name where load_set_id=%s) curve_names";
		final String sqlgetallcurvenames = "(select curve_id, mod(curve_id,1000) partition_id, curve_name, short_curve_name,curve_description from load_set2_curve_name ) curve_names";

		Properties prop = new Properties();
		prop.load(StageCurveName.class.getResourceAsStream(args[1]));

		final String LoadsetID = args[0];
		final String DB_META_URL = prop.getProperty("oracle.meta.url");
		final String DB_DW_URL = prop.getProperty("oracle.dw.url");
		final String partitionnumber= args[3];


		Map<String, String> optionsformetadata = new HashMap<String, String>();
		optionsformetadata.put("driver", "oracle.jdbc.driver.OracleDriver");
		optionsformetadata.put("url", DB_META_URL);

		if (LoadsetID.equals("all"))
		{
			optionsformetadata.put("dbtable", sqlgetallcurveattributes);
		}
		else
		{
			optionsformetadata.put("dbtable", String.format(sqlgetcurveattributesbyloadset, LoadsetID));
		}
		optionsformetadata.put("partitionColumn", "partition_id");
		optionsformetadata.put("lowerBound", "1");
		optionsformetadata.put("upperBound", "1000");
		optionsformetadata.put("numPartitions", partitionnumber);
		optionsformetadata.put("fetchSize", "1");

		// Load query result as DataFrame
		JavaRDD<Row> tempCurveMetaResult = sqlContext.read().format("jdbc").options(optionsformetadata).load()
				.javaRDD();



		JavaPairRDD<Long, Iterable<CurveMetaData>> CurveMetaList = tempCurveMetaResult
				.mapToPair(new ConvertToPairRDDForMetaData()).groupByKey();





		Map<String, String> optionsforcurvename = new HashMap<String, String>();
		optionsforcurvename.put("driver", "oracle.jdbc.driver.OracleDriver");
		optionsforcurvename.put("url", DB_DW_URL);
		if (LoadsetID.equals("all"))
		{
			optionsforcurvename.put("dbtable", sqlgetallcurvenames);
		}
		else
		{
			optionsforcurvename.put("dbtable", String.format(sqlgetcurvenamesbyloadset, LoadsetID));
		}

		optionsforcurvename.put("partitionColumn", "partition_id");
		optionsforcurvename.put("lowerBound", "1");
		optionsforcurvename.put("upperBound", "1000");
		optionsforcurvename.put("numPartitions", "1000");
		optionsforcurvename.put("fetchSize", "10000");

		// Load query result as DataFrame
		JavaRDD<Row> tempCurveNameResult = sqlContext.read().format("jdbc").options(optionsforcurvename).load()
				.javaRDD();

		JavaPairRDD<Long, CurveNames> CurveNameRDD = tempCurveNameResult.mapToPair(new ConvertToPairRDDForNames());

		//CurveMetaList.leftOuterJoin(CurveNameRDD).mapToPair(new GenerateSDI()).mapToPair(new ConvertToWritableTypes()).saveAsHadoopFile(args[2], LongWritable.class, Text.class, SequenceFileOutputFormat.class);
		
		CurveMetaList.leftOuterJoin(CurveNameRDD).mapPartitions(new GenerateSDI1()).saveAsTextFile(args[2]);

	}

}
