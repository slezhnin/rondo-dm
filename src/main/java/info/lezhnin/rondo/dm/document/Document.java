package info.lezhnin.rondo.dm.document;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import info.lezhnin.rondo.dm.Path;
import org.bson.types.ObjectId;

import java.util.Date;

/**
 * Description goes here...
 * <p/>
 * Date: 22.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class Document {
    public static final String ID = "_id";
    public static final String DOCUMENT = "rondo.dm.document";
    public static final String CREATED = "created";
    public static final String UPDATED = "updated";
    private static final Path documentPath = new Path(DOCUMENT);
    private DBObject object;

    public Document() {
        object = new BasicDBObject();
        onCreate();
        onUpdate();
    }

    public Document(DBObject object) {
        Preconditions.checkNotNull(object);
        this.object = object;
    }

    public Document(ObjectId objectId, DBCollection collection) {
        Preconditions.checkNotNull(objectId);
        Preconditions.checkNotNull(collection);
        object = collection.findOne(new BasicDBObject("_id", objectId));
    }

    public DBObject getObject() {
        return object;
    }

    public ObjectId getObjectId() {
        return (ObjectId) object.get(ID);
    }

    protected Optional<DBObject> getDocumentObject() {
        return documentPath.getDBObject(getObject());
    }

    protected DBObject getOrCreateDocumentObject() {
        return documentPath.getOrCreateDBObject(getObject());
    }

    protected void onCreate() {
        getOrCreateDocumentObject().put(CREATED, new Date());
    }

    protected void onUpdate() {
        getOrCreateDocumentObject().put(UPDATED, new Date());
    }

    public DBObject copy() {
        Document document = new Document((DBObject) ((BasicDBObject) getObject()).copy());
        document.getObject().removeField(ID);
        document.onCreate();
        document.onUpdate();
        return document.getObject();
    }

    public void load(DBCollection collection) {
        Preconditions.checkNotNull(collection);
        object = collection.findOne(new BasicDBObject(ID, getObjectId()));
    }

    public void save(DBCollection collection) {
        Preconditions.checkNotNull(collection);
        onUpdate();
        collection.save(getObject());
    }

    public Date getCreated() {
        return (Date) getDocumentObject().get().get(CREATED);
    }

    public Date getUpdated() {
        return (Date) getDocumentObject().get().get(UPDATED);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + getObject().toString();
    }
}
