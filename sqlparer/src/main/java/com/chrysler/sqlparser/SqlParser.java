/**
 * 
 */

package com.chrysler.sqlparser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import com.mongodb.*;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class SqlParser {
	private static final CCJSqlParserManager PARSER_MANAGER = new CCJSqlParserManager();
	private static final String DATABASE_NAME = "chrysler";
	public static void main(String[] args) {
		BufferedReader reader;
		System.out.println(args[0]);
		try {
			reader = new BufferedReader(new FileReader(args[0]));
			String line = reader.readLine();
			while (line != null) {
				System.out.println(line);
				line = reader.readLine();
				try {
					if (null != line && !line.equals("") && line.contains("UPDATE")) {
						Map<String,String> map = parseUpdateStatement(line);
						updateOne(map.get("tableName"),map.get("modifiedCol"), map.get("modifiedVal"),
							map.get("whereCol"), map.get("whereVal"));
					} else if (null != line && !line.equals("") && line.contains("INSERT INTO")) {
						Map<String,String> map = parseInsertStatement(line);
						insertOne(map.get("tableName"),
								Arrays.asList(map.get("cols").split(",")), 
								Arrays.asList(map.get("vals").split(",")));
					}
				} catch (JSQLParserException e) {
					e.printStackTrace();
				}
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Map<String,String> parseUpdateStatement(String sqlStr) throws JSQLParserException {
		Update update = (Update) PARSER_MANAGER.parse(new StringReader(sqlStr));
        String[] colValuePair = update.getWhere().toString().split("=");
        Map<String,String> jsonMap = new HashMap<String, String>();
        jsonMap.put("tableName", update.getTable().toString());
        jsonMap.put("whereCol", colValuePair[0]);
        jsonMap.put("whereVal", colValuePair[1]);
        jsonMap.put("modifiedCol", update.getUpdateSets().get(0).getColumns().get(0).getColumnName());
        jsonMap.put("modifiedVal", ((StringValue) update.getUpdateSets().get(0).getValues().get(0)).getValue().toString());
        System.out.println(jsonMap);
        return jsonMap;
	}

	private static Map<String,String> parseInsertStatement(String sqlStr) throws JSQLParserException {
		Insert insert= (Insert) PARSER_MANAGER.parse(new StringReader(sqlStr));
        Map<String,String> jsonMap = new HashMap<String, String>();
        jsonMap.put("tableName", insert.getTable().toString());
        String cols = "";
        for (int i = 0; i < insert.getColumns().size(); i++) {
        	cols += insert.getColumns().get(i).getColumnName() + ",";
        }
        jsonMap.put("cols", cols.substring(0,cols.length()-1));
        String vals = "";
        for (int i = 0; i < insert.getColumns().size(); i++) {
        	vals += ((StringValue) insert.getValues().getExpressions().get(i)) + ",";
        }
        jsonMap.put("vals", vals.substring(0,vals.length()-1));
        System.out.println(jsonMap);
        return jsonMap;
	}

    private static void updateOne(String collectionName, String columnName, String columnVal, String whereColumnName, String whereColumnVal) {
        MongoCollection<Document> collection = getMongoDBConnection().getCollection(collectionName);
        Document query = new Document().append(whereColumnName,  whereColumnVal);
        Bson updates = Updates.combine(
                Updates.set(columnName, columnVal));
        
        // Instructs the driver to insert a new document if none match the query
        UpdateOptions options = new UpdateOptions().upsert(true);
        try {
            UpdateResult result = collection.updateOne(query, updates, options);
            System.out.println("Modified document count: " + result.getModifiedCount());
            System.out.println("Upserted id: " + result.getUpsertedId());
        } catch (MongoException me) {
            System.err.println("Unable to update due to an error: " + me);
        	me.printStackTrace();
        }
    }

    private static void insertOne(String collectionName, List<String> columnNames, List<String> columnVals) {
        MongoCollection<Document> collection = getMongoDBConnection().getCollection(collectionName);
        Document doc = new Document();//.append("PHONE_NUMBER","0");
        for (int i=0; i< columnNames.size(); i++) {
        	System.out.println(columnNames.get(i) + "=" + columnVals.get(i));
        	doc.append(columnNames.get(i), columnVals.get(i));
        }
        try {
            InsertOneResult result = collection.insertOne(doc);
            System.out.println("Inserted document count: " + result.getInsertedId());
        } catch (MongoException me) {
            System.err.println("Unable to insert due to an error: " + me);
        	me.printStackTrace();
        }
    }

    private static MongoDatabase getMongoDBConnection() {
        String uri= "mongodb+srv://dbUser:dbUserPassword@atlascluster.wuqf4tn.mongodb.net/?appName=AtlasCluster";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
            return database;
            
            } catch (MongoException me) {
                System.err.println("Unable to insert due to an error: " + me);
            	me.printStackTrace();
            }
		return null;
    }
    
    private static MongoDatabase getMongoConnection() {
    	List<Map<String, String>> data = 
    			new ArrayList()

    	// Retrieves documents that match the filter, applying a projection and a descending sort to the results
        MongoCursor<Document> cursor = getMongoDBConnection().getCollection("CALLER_ID_DATA").find()
                .sort(Sorts.descending("PHONE_NUMBER")).iterator();
        // Prints the results of the find operation as JSON
        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }    }    
    
}
