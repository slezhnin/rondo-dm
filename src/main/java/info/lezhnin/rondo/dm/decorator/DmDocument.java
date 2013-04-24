package info.lezhnin.rondo.dm.decorator;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import info.lezhnin.rondo.dm.DmObject;
import info.lezhnin.rondo.dm.DmPath;
import org.bson.types.ObjectId;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * Description goes here...
 * <p/>
 * Date: 22.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class DmDocument implements DmObject {
    public static final String DOCUMENT = "rondo_dm.document";
    public static final String CREATED = "created";
    public static final String UPDATED = "updated";
    private static final DmPath documentPath = new DmPath(DOCUMENT);
    private DmObject dmObject;

    public DmDocument(DmObject dmObject) {
        Preconditions.checkNotNull(dmObject);
        this.dmObject = dmObject;
    }

    public DmObject getDmObject() {
        return dmObject;
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

    public DmObject copy() {
        DmDocument document = new DmDocument(dmObject.copy());
        document.onCreate();
        document.onUpdate();
        return document;
    }

    public DBObject getObject() {
        return dmObject.getObject();
    }

    public DBCollection getCollection() {
        return dmObject.getCollection();
    }

    public void setCollection(@Nullable DBCollection collection) {
        dmObject.setCollection(collection);
    }

    @Nullable
    public ObjectId getObjectId() {
        return dmObject.getObjectId();
    }

    public void save() {
        onUpdate();
        getDmObject().save();
    }

    public boolean load(@Nullable ObjectId objectId) {
        return dmObject.load(objectId);
    }

    public Date getCreated() {
        return (Date) getDocumentObject().get().get(CREATED);
    }

    public Date getUpdated() {
        return (Date) getDocumentObject().get().get(UPDATED);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + dmObject.toString();
    }
}
