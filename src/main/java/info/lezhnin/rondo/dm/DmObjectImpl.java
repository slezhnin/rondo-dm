package info.lezhnin.rondo.dm;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import javax.annotation.Nullable;

/**
 * Description goes here...
 * <p/>
 * Date: 24.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class DmObjectImpl implements DmObject {

    private DBObject object;
    private Optional<DBCollection> collection;

    public DmObjectImpl(ObjectId objectId, DBCollection collection) {
        Preconditions.checkNotNull(objectId);
        Preconditions.checkNotNull(collection);
        setCollection(collection);
        if (!load(objectId)) object = new BasicDBObject(ID, objectId);
    }

    public DmObjectImpl() {
        this(null);
    }

    public DmObjectImpl(@Nullable DBCollection collection) {
        this(new BasicDBObject(), collection);
    }

    public DmObjectImpl(DBObject object, @Nullable DBCollection collection) {
        Preconditions.checkNotNull(object);
        this.object = object;
        setCollection(collection);
    }

    public DBObject getObject() {
        return object;
    }

    public DBCollection getCollection() {
        return collection.get();
    }

    public void setCollection(@Nullable DBCollection collection) {
        this.collection = Optional.fromNullable(collection);
    }

    public
    @Nullable
    ObjectId getObjectId() {
        return ObjectId.massageToObjectId(object.get(ID));
    }

    public void save() {
        getCollection().save(getObject());
    }

    public boolean load(@Nullable ObjectId objectId) {
        ObjectId id = objectId == null ? getObjectId() : objectId;
        DBObject loadedObject = getCollection().findOne(new BasicDBObject(ID, id));
        if (loadedObject == null) return false;
        object = loadedObject;
        return true;
    }

    public DmObject copy() {
        DBObject copyObject = (DBObject) ((BasicDBObject) getObject()).copy();
        copyObject.removeField(ID);
        return new DmObjectImpl(copyObject, collection.orNull());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + getObject().toString();
    }
}
