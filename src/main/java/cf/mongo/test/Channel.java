package cf.mongo.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Indexed;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class Channel {		
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

	public List<Tag> getTags() {
		return tags;
	}
	
	//TODO not thread safe
	public void setTags(List<Tag> tags){
		this.tags = tags;
	}

	public List<Property> getProperties() {
		return properties;
	}
	
	//TODO not thread safe
	public void setProperties(List<Property> properties){
		this.properties = properties;
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