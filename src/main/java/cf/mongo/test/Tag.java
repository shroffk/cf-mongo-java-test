package cf.mongo.test;

import org.mongodb.morphia.annotations.Indexed;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Tag {
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
