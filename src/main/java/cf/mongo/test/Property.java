package cf.mongo.test;

import org.mongodb.morphia.annotations.Indexed;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Property {
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