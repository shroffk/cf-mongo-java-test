import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.query.MorphiaIterator;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateResults;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

public class Test {

	private static DB database;
	private static MongoClient mongoClient;
	private static List<Property> createdProperties;
	private static Datastore ds;
	private static Morphia morphia;

	public static void main(String[] args) {

		try {
			mongoClient = new MongoClient("130.199.219.160", 27017);
			List<String> dbNames = mongoClient.getDatabaseNames();
			for (String dbName : dbNames) {
				System.out.println(dbName);
			}
			database = mongoClient.getDB("cf");

			DBCollection tags = database.getCollection("tags");
			DBCollection properties = database.getCollection("properties");
			DBCollection channels = database.getCollection("channels");

			List<Tag> testTags = new ArrayList<Test.Tag>();
			for (int i = 0; i < 100; i++) {
				testTags.add(new Tag("testTag" + i, "me"));
			}
			List<Property> testProps = new ArrayList<Test.Property>();
			for (int i = 0; i < 100; i++) {
				testProps.add(new Property("testProperty" + i, String
						.valueOf(i), "me"));
			}
			List<Channel> testChannels = new ArrayList<Test.Channel>();
			for (int i = 0; i < 1000; i++) {
				testChannels.add(new Channel("testChannel" + i, "me", Arrays
						.asList(testTags.get(i % 100)), Arrays.asList(testProps
						.get(i % 100))));
			}

	//		ds = new Morphia().map(Tag.class).map(Property.class).map(Channel.class).createDatastore(mongoClient, "cf");

			morphia = new Morphia();
//			morphia.map(Tag.class);
//			morphia.map(Property.class);
//			morphia.map(Channel.class);			
						
			ds = morphia.createDatastore(mongoClient, "cf");
//			cleanChannels();
			
			ds.save(testTags);
			ds.save(testProps);
			ds.save(testChannels);

//			Query<Channel> result = ds.createQuery(Channel.class).field("name")
//					.containsIgnoreCase("5");
//			for (Channel c : result) {
//				System.out.println(c.getName());
//			}
//
//			Query<Channel> query = ds.createQuery(Channel.class);
//
//			query.or(
//					query.criteria("name").containsIgnoreCase("5"),
//					query.criteria("name").containsIgnoreCase("3")
//					);
//			for (Channel c : query) {
//				System.out.println(c.getName());
//			}
//			
//			Query<Channel> completeQuery = ds.createQuery(Channel.class);
//			completeQuery.and(
//					completeQuery.criteria("tags.name").containsIgnoreCase("5"),
//					completeQuery.criteria("properties.name").containsIgnoreCase("5")
//					);
//			for (Channel c : completeQuery) {
//				System.out.println(c.getName());
//				for (Tag tag : c.getTags()) {
//					System.out.println(tag.getName());
//				}
//				for (Property prop : c.getProperties()) {
//					System.out.println(prop.getName() + ":" + prop.getValue());
//				}
//			}
//			populateData();

			query();
			rawQueries();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void populateData() {
		// cleanup 
//		cleanChannels();
		
		for (int i = 10; i < 70; i++) {
			Property prop = new Property("prop"+i, String.valueOf(i), "propOwner");
			ds.save(prop);			
		}
		
		createdProperties = ds.createQuery(Property.class).asList();
		
		for (int i = 1; i < 101; i++) {			
			String cell = String.format("%03d", i);
			long t = System.currentTimeMillis();			
			insertSRCell(cell);
			System.out.println(System.currentTimeMillis() - t + " : sr_cell " + cell);
		}
		
		for (int i = 1; i < 101; i++) {			
			String cell = String.format("%03d", i);
			long t = System.currentTimeMillis();
			insertBOCell(cell);
			System.out.println(System.currentTimeMillis() - t + " : bo_cell " + cell);
		}
		
	}

	private static void query() {
		Query<Channel> query = ds.createQuery(Channel.class);
		
		long startTime = System.currentTimeMillis();
		List<Channel> result = query.asList();
		System.out.println(result.size()+ " : " + (System.currentTimeMillis() - startTime));
		
		startTime = System.currentTimeMillis();
		result = query.field("name").containsIgnoreCase("SR").asList();
		System.out.println(result.size()+ " : " + (System.currentTimeMillis() - startTime));
		
		startTime = System.currentTimeMillis();
		Query<Channel> interator = query.field("name").containsIgnoreCase("SR.*C008");
		System.out.println(interator.countAll() + " : " + (System.currentTimeMillis() - startTime));
		
		startTime = System.currentTimeMillis();
		result = query.field("name").containsIgnoreCase("SR.*C008").asList();
		System.out.println(result.size()+ " : " + (System.currentTimeMillis() - startTime));		
		
		// bulk operations
		startTime = System.currentTimeMillis();
		Pattern searchPattern = Pattern.compile("SR.*C008", Pattern.CASE_INSENSITIVE);	
		Query<Test.Channel> updateQuery = ds.createQuery(Test.Channel.class).field("name").containsIgnoreCase(searchPattern.pattern());
		UpdateResults<Channel> updateResult = ds.update(
				updateQuery,
				ds.createUpdateOperations(Channel.class).add("tags", new Tag("testMorphiaTag", "me"), false));
		System.out.println("update" + updateResult.getError()+ " : " + (System.currentTimeMillis() - startTime));
		System.out.println(updateResult.toString());
		
		startTime = System.currentTimeMillis();
		result = ds.createQuery(Channel.class).field("tags.name").containsIgnoreCase("testMorphiaTag").asList();
		System.out.println("update result: " + result.size()+ " : " + (System.currentTimeMillis() - startTime));
		
//		startTime = System.currentTimeMillis();
//		updateResult = ds.update(
//				updateQuery,
//				ds.createUpdateOperations(Channel.class).removeAll("tags.name", "testMorphiaTag"));
//		System.out.println("cleanup: " + updateResult.getError()+ " : " + (System.currentTimeMillis() - startTime));
//		
		startTime = System.currentTimeMillis();
		result = ds.createQuery(Channel.class).field("tags.name").containsIgnoreCase("testMorphiaTag").asList();
		System.out.println("cleanup result: " + result.size()+ " : " + (System.currentTimeMillis() - startTime));
	}
	
	private static void rawQueries(){
		Set<String> collections = database.getCollectionNames();
		DBCollection channels = database.getCollection("Channel");
		
		long startTime = System.currentTimeMillis();
		// Insert
		List<Tag> testTags = new ArrayList<Test.Tag>();
		for (int i = 0; i < 100; i++) {
			testTags.add(new Tag("rawTestTag" + i, "me"));
		}
		List<Property> testProps = new ArrayList<Test.Property>();
		for (int i = 0; i < 100; i++) {
			testProps.add(new Property("rawTestProperty" + i, String
					.valueOf(i), "me"));
		}
		List<DBObject> testChannels = new ArrayList<DBObject>();
		for (int i = 0; i < 1000; i++) {
			testChannels.add(new Channel("rawTestChannel" + i, "me", Arrays
					.asList(testTags.get(i % 100)), Arrays.asList(testProps
					.get(i % 100))).getBasicDBObject());
		}
		startTime = System.currentTimeMillis();
		channels.insert(testChannels);
		System.out.println("Insert : " + (System.currentTimeMillis() - startTime));
		
		Pattern searchPattern = Pattern.compile("SR.*C007", Pattern.CASE_INSENSITIVE);		
		BasicDBObject simpleQuery = new BasicDBObject("name", searchPattern);
		startTime = System.currentTimeMillis();
		DBCursor cursor = channels.find(simpleQuery);
		System.out.println(cursor.size()+ " : " + (System.currentTimeMillis() - startTime));
		BulkWriteOperation builder = channels.initializeOrderedBulkOperation();		
		try {
		    while (cursor.hasNext()) {
		    	Channel ch = new Channel();
		    	DBObject dbObject = cursor.next();
		    	ch.fromBasicDBObject(dbObject);
				ch.tags.add(new Tag("testRawTag", "me"));
		//    	System.out.println(ch.getName());
				builder.find(new BasicDBObject("name", ch.getName())).updateOne(new BasicDBObject("$set", ch.getBasicDBObject()));
		    }
		} finally {
		    cursor.close();
		}
		BulkWriteResult result = builder.execute();
		System.out.println(result.toString() + " : " + (System.currentTimeMillis() - startTime));
				
		// search				
		startTime = System.currentTimeMillis();
		cursor = channels.find(simpleQuery);
		System.out.println(cursor.size()+ " : " + (System.currentTimeMillis() - startTime));
	}

	private static void cleanChannels() {
		database.getCollection("tags").drop();
		database.getCollection("properties").drop();
		database.getCollection("channels").drop();
				
		ds.delete(ds.createQuery(Property.class));
		ds.delete(ds.createQuery(Tag.class));
		ds.delete(ds.createQuery(Channel.class));
	}
	

	private static void insertSRCell(String cell) {
		// TODO Auto-generated method stub
		TokenGenerator tokenGenerator = TokenGenerator.getTokenGenerator(1000);
		int channelCount = 0;
		
		String pre = "SR:C";
		String loc = "storage ring";
		insert_big_magnets(2, pre, "DP", loc, cell, "dipole", channelCount, tokenGenerator);
		insert_big_magnets(5, pre, "QDP:D", loc, cell, "private static voidocusing quadrupole", channelCount, tokenGenerator);
	    insert_big_magnets(5, pre, "QDP:F", loc, cell, "focusing quadrupole", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "QDP:S", loc, cell, "skew quadrupole", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "STP", loc, cell, "sextupole", channelCount, tokenGenerator);
	    insert_big_magnets(5, pre, "HC:S", loc, cell, "horizontal slow corrector", channelCount, tokenGenerator);
	    insert_air_magnets(5, pre, "HC:F", loc, cell, "horizontal fast corrector", channelCount, tokenGenerator);
	    insert_big_magnets(5, pre, "VC:S", loc, cell, "vertical slow corrector", channelCount, tokenGenerator);
	    insert_air_magnets(4, pre, "VC:F", loc, cell, "vertical fast corrector", channelCount, tokenGenerator);

	    insert_valves(5, pre, "GV", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_gauges(5, pre, "VGC", loc, cell, "vacuum", channelCount, tokenGenerator);
	    insert_gauges(5, pre, "TCG", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_pumps(2, pre, "IPC", loc, cell, "vacuum", channelCount, tokenGenerator);
	    insert_pumps(2, pre, "TMP", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_temps(40, pre, "TC", loc, cell, "temperature sensor", channelCount, tokenGenerator);

	    insert_bpms(4, pre, "BSA", loc, cell, "small aperture BPM", channelCount, tokenGenerator);
	    insert_bpms(4, pre, "BHS", loc, cell, "high stability BPM", channelCount, tokenGenerator);
	    insert_bpms(4, pre, "BLA", loc, cell, "large aperture BPM", channelCount, tokenGenerator);
		
	}

	
	private static void  insertBOCell(String cell){
	    String loc = "booster";
	    String pre = "BR:C";
	    int channelCount = 0;
	    
	    TokenGenerator tokenGenerator = TokenGenerator.getTokenGenerator(500);

	    insert_big_magnets(2, pre, "DP", loc, cell, "dipole", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "QDP:D", loc, cell, "defocusing quadrupole", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "QDP:F", loc, cell, "focusing quadrupole", channelCount, tokenGenerator);
	    insert_big_magnets(2, pre, "STP", loc, cell, "sextupole", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "HC", loc, cell, "horizontal corrector", channelCount, tokenGenerator);
	    insert_big_magnets(4, pre, "VC", loc, cell, "vertical corrector", channelCount, tokenGenerator);

	    insert_valves(4, pre, "GV", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_gauges(4, pre, "VGC", loc, cell, "vacuum", channelCount, tokenGenerator);
	    insert_gauges(2, pre, "TCG", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_pumps(2, pre, "IPC", loc, cell, "vacuum", channelCount, tokenGenerator);
	    insert_pumps(2, pre, "TMP", loc, cell, "vacuum", channelCount, tokenGenerator);

	    insert_temps(10, pre, "TC", loc, cell, "temperature sensor", channelCount, tokenGenerator);

	    insert_bpms(2, pre, "BLA", loc, cell, "beam position monitor", channelCount, tokenGenerator);
	}
	
	private static void insert_big_magnets(int count, String prefix, String dev,
			String loc, String cell, String element, int channelCount, TokenGenerator tokenGenerator) {
		insert_bunch(count, prefix, "PS:", "{"+dev+"}I-RB", loc, cell, element, "power supply", "current", "readback", channelCount, tokenGenerator);
		insert_bunch(count, prefix, "PS:", "{"+dev+"}I-SP", loc, cell, element, "power supply", "current", "setpoint", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}On-Sw", loc, cell, element, "power supply", "power", "switch", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Rst-Cmd", loc, cell, element, "power supply", "reset", "command", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}On-St", loc, cell, element, "power supply", "power", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Acc-St", loc, cell, element, "power supply", "access", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}OK-St", loc, cell, element, "power supply", "sum error", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}T-St", loc, cell, element, "power supply", "temperature", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}F-St", loc, cell, element, "power supply", "water flow", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Gnd-St", loc, cell, element, "power supply", "ground", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Ctl-St", loc, cell, element, "power supply", "control", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Val-St", loc, cell, element, "power supply", "value", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}Fld-RB", loc, cell, element, "magnet", "field", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}Fld-SP", loc, cell, element, "magnet", "field", "setpoint", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}T:1-RB", loc, cell, element, "magnet", "temperature", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}T:2-RB", loc, cell, element, "magnet", "temperature", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}F-RB", loc, cell, element, "magnet", "water flow", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}F:in-St", loc, cell, element, "magnet", "water flow in", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}F:out-St", loc, cell, element, "magnet", "water flow out", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}F:dif-St", loc, cell, element, "magnet", "water flow diff", "status", channelCount, tokenGenerator);
	}

	private static void insert_air_magnets(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}I-RB", loc, cell, element, "power supply", "current", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}I-SP", loc, cell, element, "power supply", "current", "setpoint", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}On-Sw", loc, cell, element, "power supply", "power", "switch", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Rst-Cmd", loc, cell, element, "power supply", "reset", "command", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}On-St", loc, cell, element, "power supply", "power", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}Acc-St", loc, cell, element, "power supply", "access", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PS:", "{"+dev+"}OK-St", loc, cell, element, "power supply", "sum error", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}Fld-RB", loc, cell, element, "magnet", "field", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}Fld-SP", loc, cell, element, "magnet", "field", "setpoint", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "MG:", "{"+dev+"}T-RB", loc, cell, element, "magnet", "temperature", "readback", channelCount, tokenGenerator);}

	private static void insert_valves(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}Opn-Sw", loc, cell, element, "valve", "position", "switch", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}Opn-St", loc, cell, element, "valve", "position", "status", channelCount, tokenGenerator);}

	private static void insert_gauges(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}P-RB", loc, cell, element, "gauge", "pressure", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}OK-St", loc, cell, element, "gauge", "error", "status", channelCount, tokenGenerator);}

	private static void insert_pumps(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}I-RB", loc, cell, element, "pump", "current", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}P-RB", loc, cell, element, "pump", "pressure", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}On-Sw", loc, cell, element, "pump", "power", "switch", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}OK-St", loc, cell, element, "pump", "error", "status", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "VA:", "{"+dev+"}On-St", loc, cell, element, "pump", "power", "status", channelCount, tokenGenerator);}

	private static void insert_temps(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "PU:T", "{"+dev+"}T:1-RB", loc, cell, element, "sensor", "temperature 1", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PU:T", "{"+dev+"}T:2-RB", loc, cell, element, "sensor", "temperature 2", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PU:T", "{"+dev+"}T:3-RB", loc, cell, element, "sensor", "temperature 3", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PU:T", "{"+dev+"}T:4-RB", loc, cell, element, "sensor", "temperature 4", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "PU:T", "{"+dev+"}On-St", loc, cell, element, "sensor", "power", "status", channelCount, tokenGenerator);}

	private static void insert_bpms(int count, String prefix, String dev, String loc, String cell, String element , int channelCount, TokenGenerator tokenGenerator) {
	    insert_bunch(count, prefix, "BI:", "{"+dev+"}Pos:X-RB", loc, cell, element, "bpm", "x position", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "BI:", "{"+dev+"}Pos:Y-RB", loc, cell, element, "bpm", "y position", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "BI:", "{"+dev+"}Sig:X-RB", loc, cell, element, "bpm", "x sigma", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "BI:", "{"+dev+"}Sig:Y-RB", loc, cell, element, "bpm", "y sigma", "readback", channelCount, tokenGenerator);
	    insert_bunch(count, prefix, "BI:", "{"+dev+"}On-St", loc, cell, element, "bpm", "power", "status", channelCount, tokenGenerator);}
	    
	private static void insert_bunch(int count, String prefix, String midfix,
			String postfix, String loc, String cell, String element,
			String device, String unit, String sigtype, int channelCount, TokenGenerator tokenGenerator) {
		
		Key<Channel> ch = null;
		for (int i = 1; i < count+1; i++) {
			String name = prefix+cell+"-"+midfix+postfix;
			ch = ds.save(new Channel(name, "cOwner", Collections.<Tag>emptyList(), Collections.<Property>emptyList()));
		
			Query<Channel> updateQuery = ds.createQuery(Channel.class)
					.field("name").equal(name);
			
			List<Property> properties = new ArrayList<Property>();
			List<Tag> tags = new ArrayList<Tag>();
			
			if (ch != null) {
				properties.add(new Property("location", loc, "pOwner"));
				properties.add(new Property("cell", cell, "pOwner"));
				properties.add(new Property("element", element, "pOwner"));
				properties.add(new Property("device", device, "pOwner"));
				properties.add(new Property("unit", unit, "pOwner"));
				properties.add(new Property("type", sigtype, "pOwner"));
				String mount = "center";

				if (postfix.endsWith("}T:1-RB")) {
					mount = "outside";
				} else if (postfix.endsWith("}T:2-RB")) {
					mount = "inside";
				} else if (postfix.endsWith("}T:3-RB")) {
					mount = "top";
				} else if (postfix.endsWith("}T:4-RB")) {
					mount = "bottom";
				}
				properties.add(new Property("mount", mount, "pOwner"));
				
				
				// group0-5
				for (int j = 0; j < 6; j++) {					
					properties.add(new Property("group" + j, String.valueOf(j), "pOwner"));
					tags.add(new Tag("group" + j, "pOwner"));
				}
				// group6-9
		        if (channelCount%2 == 1){
		        	properties.add(new Property("group6","500", "pOwner"));
		            tags.add(new Tag("group6-500","tOwner"));
		        } else if (channelCount <= 2*200){
		            properties.add(new Property("group6","200", "pOwner"));
		            tags.add(new Tag("group6-200","tOwner"));
		        } else if (channelCount <= 2*(200+100)){
		            properties.add(new Property("group6","100", "pOwner"));
		            tags.add(new Tag("group6-100","tOwner"));
		        } else if (channelCount <= 2*(200+100+50)){
		            properties.add(new Property("group6","50", "pOwner"));
		            tags.add(new Tag("group6-50","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20)){
		            properties.add(new Property("group6","20", "pOwner"));
		            tags.add(new Tag("group6-20","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10)){
		            properties.add(new Property("group6","10", "pOwner"));
		            tags.add(new Tag("group6-10","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5)){
		            properties.add(new Property("group6","5", "pOwner"));
		            tags.add(new Tag("group6-5","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5+2)){
		            properties.add(new Property("group6","2", "pOwner"));
		            tags.add(new Tag("group6-2","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5+2+1)){
		            properties.add(new Property("group6","1", "pOwner"));
		            tags.add(new Tag("group6-1","tOwner"));
		        } else{
		            properties.add(new Property("group6","0", "pOwner"));
		            tags.add(new Tag("group6-0","tOwner"));
		        }
		        if (channelCount%2 == 0){
		            properties.add(new Property("group7","500", "pOwner"));
		            tags.add(new Tag("group7-500","tOwner"));
		        } else if (channelCount <= 2*200){
		            properties.add(new Property("group7","200", "pOwner"));
		            tags.add(new Tag("group7-200","tOwner"));
		        } else if (channelCount <= 2*(200+100)){
		            properties.add(new Property("group7","100", "pOwner"));
		            tags.add(new Tag("group7-100","tOwner"));
		        } else if (channelCount <= 2*(200+100+50)){
		            properties.add(new Property("group7","50", "pOwner"));
		            tags.add(new Tag("group7-50","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20)){
		            properties.add(new Property("group7","20", "pOwner"));
		            tags.add(new Tag("group7-20","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10)){
		            properties.add(new Property("group7","10", "pOwner"));
		            tags.add(new Tag("group7-10","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5)){
		            properties.add(new Property("group7","5", "pOwner"));
		            tags.add(new Tag("group7-5","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5+2)){
		            properties.add(new Property("group7","2", "pOwner"));
		            tags.add(new Tag("group7-2","tOwner"));
		        } else if (channelCount <= 2*(200+100+50+20+10+5+2+1)){
		            properties.add(new Property("group7","1", "pOwner"));
		            tags.add(new Tag("group7-1","tOwner"));
		        } else{
		            properties.add(new Property("group7","0", "pOwner"));
		            tags.add(new Tag("group7-0","tOwner"));
		        }
		        if (channelCount <= 500){
		            properties.add(new Property("group8","500", "pOwner"));
		            tags.add(new Tag("group8-500","tOwner"));
		        } else if (channelCount <= 500+200){
		            properties.add(new Property("group8","200", "pOwner"));
		            tags.add(new Tag("group8-200","tOwner"));
		        } else if (channelCount <= 500+(200+100)){
		            properties.add(new Property("group8","100", "pOwner"));
		            tags.add(new Tag("group8-100","tOwner"));
		        } else if (channelCount <= 500+(200+100+50)){
		            properties.add(new Property("group8","50", "pOwner"));
		            tags.add(new Tag("group8-50","tOwner"));
		        } else if (channelCount <= 500+(200+100+50+20)){
		            properties.add(new Property("group8","20", "pOwner"));
		            tags.add(new Tag("group8-20","tOwner"));
		        } else if (channelCount <= 500+(200+100+50+20+10)){
		            properties.add(new Property("group8","10", "pOwner"));
		            tags.add(new Tag("group8-10","tOwner"));
		        } else if (channelCount <= 500+(200+100+50+20+10+5)){
		            properties.add(new Property("group8","5", "pOwner"));
		            tags.add(new Tag("group8-5","tOwner"));
		        } else if (channelCount <= 500+(200+100+50+20+10+5+2)){
		            properties.add(new Property("group8","2", "pOwner"));
		            tags.add(new Tag("group8-2","tOwner"));
		        } else if (channelCount <= 500+(200+100+50+20+10+5+2+1)){
		            properties.add(new Property("group8","1", "pOwner"));
		            tags.add(new Tag("group8-1","tOwner"));
		        } else{
		            properties.add(new Property("group8","0", "pOwner"));
		            tags.add(new Tag("group8-0","tOwner"));
		        }
		        if (channelCount > 500){
		            properties.add(new Property("group9","500", "pOwner"));
		            tags.add(new Tag("group9-500","tOwner"));
		        } else if (channelCount > 500-200){
		            properties.add(new Property("group9","200", "pOwner"));
		            tags.add(new Tag("group9-200","tOwner"));
		        } else if (channelCount > 500-200-100){
		            properties.add(new Property("group9","100", "pOwner"));
		            tags.add(new Tag("group9-100","tOwner"));
		        } else if (channelCount > 500-200-100-50){
		            properties.add(new Property("group9","50", "pOwner"));
		            tags.add(new Tag("group9-50","tOwner"));
		        } else if (channelCount > 500-200-100-50-20){
		            properties.add(new Property("group9","20", "pOwner"));
		            tags.add(new Tag("group9-20","tOwner"));
		        } else if (channelCount > 500-200-100-50-20-10){
		            properties.add(new Property("group9","10", "pOwner"));
		            tags.add(new Tag("group9-10","tOwner"));
		        } else if (channelCount > 500-200-100-50-20-10-5){
		            properties.add(new Property("group9","5", "pOwner"));
		            tags.add(new Tag("group9-5","tOwner"));
		        } else if (channelCount > 500-200-100-50-20-10-5-2){
		            properties.add(new Property("group9","2", "pOwner"));
		            tags.add(new Tag("group9-2","tOwner"));
		        } else if (channelCount > 500-200-100-50-20-10-5-2-1){
		            properties.add(new Property("group9","1", "pOwner"));
		            tags.add(new Tag("group9-1","tOwner"));
		        }else{
		            properties.add(new Property("group9","0", "pOwner"));
		            tags.add(new Tag("group9-0","tOwner"));
		        }
//		        for p in range(20,max_prop){
//		            properties.add(new Property("prop"+`p`.zfill(2),`channelCount`+"-"+`p`.zfill(2))
//		        for p in range(11,max_tag){
//		            tags.add(new Tag("tag"+`p`.zfill(2),"tOwner"));
		        
		        if (Integer.valueOf(cell)%9 == 0){
		            tags.add(new Tag("tagnine","tOwner"));
		        } else if (Integer.valueOf(cell)%8 == 0){
		            tags.add(new Tag("tageight","tOwner"));
		        } else if (Integer.valueOf(cell)%7 == 0){
		            tags.add(new Tag("tagseven","tOwner"));
		        } else if (Integer.valueOf(cell)%6 == 0){
		            tags.add(new Tag("tagsix","tOwner"));
		        } else if (Integer.valueOf(cell)%5 == 0){
		            tags.add(new Tag("tagfive","tOwner"));
		        } else if (Integer.valueOf(cell)%4 == 0){
		            tags.add(new Tag("tagfour","tOwner"));
		        } else if (Integer.valueOf(cell)%3 == 0){
		            tags.add(new Tag("tagthree","tOwner"));
		        } else if (Integer.valueOf(cell)%2 == 0){
		            tags.add(new Tag("tagtwo","tOwner"));
		        }else{
		            tags.add(new Tag("tagone","tOwner"));
		        }
		        
		        ds.update(
						updateQuery,
						ds.createUpdateOperations(Channel.class).addAll(
								"properties", properties, false));
		        ds.update(
						updateQuery,
						ds.createUpdateOperations(Channel.class).addAll(
								"tags", tags, false));
		        
		        
		        
			}
			
		}	
		
	}
	
	public static class TokenGenerator{
		
		private final Map<Integer, Integer> tokens;
		private int tokenCount;
		private Random r;
		
		private TokenGenerator(Map<Integer, Integer> tokens, int tokenCount) {
			this.tokens = tokens;
			this.tokenCount = tokenCount;
			this.r = new Random();
		}

		/**
		 * 
		 * @param tokenCount
		 *            1000 or 500
		 * @return
		 */
		public static TokenGenerator getTokenGenerator(int tokenCount){
			HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
			map.put(0, 112);
			map.put(1, 1);
			map.put(2, 2);
			map.put(5, 5);
			map.put(10, 10);
			map.put(20, 20);
			map.put(50, 50);
			map.put(100, 100);
			map.put(200, 200);
			if(tokenCount == 1000){
				map.put(500, 500);
			}		
			return new TokenGenerator(map, tokenCount);
		}
		
		public Integer popRandomToken(){
			Integer randomKey = r.nextInt(tokens.size());
			if(tokens.get(randomKey) > 0){
				tokens.put(randomKey, tokens.get(randomKey) - 1);
				tokenCount--;
				return randomKey;
			}else if(tokenCount > 0){
				return popRandomToken();
			}else{
				return null;
			}
		}
		
	}
	
	public static class Tag {
		@Indexed
		private String name;
		private String owner;

		public Tag() {

		}

		public Tag(String name, String owner) {
			super();
			this.name = name;
			this.owner = owner;
		}

		public String getName() {
			return name;
		}

		public String getOwner() {
			return owner;
		}
		
		public BasicDBObject getBasicDBObject()
		{
			BasicDBObject num = new BasicDBObject();
			num.put("name", this.name );
			num.put("owner", this.owner); 
			return num;
		}

		public void fromBasicDBObject(DBObject dbObject) {
			this.name = (String) dbObject.get("name");
			this.owner  = (String) dbObject.get("owner");
		}
	}

	public static class Property {
		@Indexed		
		private String name;
		@Indexed
		private String value;
		private String owner;

		public Property() {

		}

		public Property(String name, String value, String owner) {
			super();
			this.name = name;
			this.value = value;
			this.owner = owner;
		}

		public String getName() {
			return name;
		}

		public String getValue() {
			return value;
		}

		public String getOwner() {
			return owner;
		}
		
		public BasicDBObject getBasicDBObject()
		{
			BasicDBObject num = new BasicDBObject();
			num.put("name", this.name );
			num.put("value", this.value );
			num.put("owner", this.owner); 
			return num;
		}

		public void fromBasicDBObject(DBObject dbObject) {
			this.name = (String) dbObject.get("name");
			this.name = (String) dbObject.get("value");
			this.owner  = (String) dbObject.get("owner");
		}

	}

	private static class Channel {		
		@Indexed		
		private String name;
		private String owner;
		@Indexed
		@Embedded
		private List<Tag> tags;
		@Indexed
		@Embedded
		private List<Property> properties;

		public Channel() {

		}

		public Channel(String name, String owner, List<Tag> tags,
				List<Property> properties) {
			super();
			this.name = name;
			this.owner = owner;
			this.tags = tags;
			this.properties = properties;
		}

		public String getName() {
			return name;
		}

		public String getOwner() {
			return owner;
		}

		public Collection<Tag> getTags() {
			return tags;
		}

		public Collection<Property> getProperties() {
			return properties;
		}
		
		public BasicDBObject getBasicDBObject()
		{
			BasicDBObject num = new BasicDBObject();
			num.put("name", this.name );
			num.put("owner", this.owner); 
			DBObject tagList = new BasicDBList();
			for (Tag tag: this.tags) {
				tagList.put(String.valueOf(this.tags.indexOf(tag)), tag.getBasicDBObject());
			}
			num.put("tags", tagList); 
			DBObject propertyList = new BasicDBList();
			for (Property property: this.properties) {
				propertyList.put(String.valueOf(this.properties.indexOf(property)), property.getBasicDBObject());
			}
			num.put("properties", propertyList);
			return num;
		}
		
		public void fromBasicDBObject(DBObject dbObject){
			this.name = (String) dbObject.get("name");
			this.owner  = (String) dbObject.get("owner");
			this.properties = new ArrayList<Property>();
			for(Object p : ((BasicDBList) dbObject.get("properties"))){
				Property prop = new Property();
				prop.fromBasicDBObject((DBObject) p);
				this.properties.add(prop);
			}
			this.tags = new ArrayList<Tag>();
			for(Object t : ((BasicDBList) dbObject.get("tags"))){
				Tag tag = new Tag();
				tag.fromBasicDBObject((DBObject) t);
				this.tags.add(tag);
			}
		}

	}
}
