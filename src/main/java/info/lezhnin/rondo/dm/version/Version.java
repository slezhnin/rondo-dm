package info.lezhnin.rondo.dm.version;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import info.lezhnin.rondo.dm.Path;
import info.lezhnin.rondo.dm.document.Document;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.List;

/**
 * Description goes here...
 * <p/>
 * Date: 20.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class Version extends Document {
    public static final String VERSION = "rondo.dm.version";
    public static final String LABELS = "labels";
    public static final String PARENTS = "parents";
    public static final String CURRENT_LABEL = "current";
    private static final Path versionPath = new Path(VERSION);

    public Version() {
        super();
    }

    public Version(DBObject object) {
        super(object);
    }

    public Version(ObjectId objectId, DBCollection collection) {
        super(objectId, collection);
    }

    protected Optional<DBObject> getVersionObject() {
        return versionPath.getDBObject(getObject());
    }

    protected DBObject getOrCreateVersionObject() {
        return versionPath.getOrCreateDBObject(getObject());
    }

    public DBObject create() {
        Version newVersion = new Version(copy());
        DBObject newVersionObject = newVersion.getOrCreateVersionObject();
        List labels = isVersioned() ? getLabels() : new BasicDBList();
        newVersionObject.put(LABELS, labels);
        List parents = isVersioned() ? getParentIds() : new BasicDBList();
        parents.add(((BasicDBObject) getObject()).getObjectId(ID));
        newVersionObject.put(PARENTS, parents);
        return newVersion.getObject();
    }

    public boolean isVersioned() {
        Optional<DBObject> version = getVersionObject();
        return version.isPresent() && version.get().containsField(LABELS) && version.get().containsField(PARENTS);
    }

    public List<String> getLabels() {
        return ImmutableList.copyOf((Collection) getVersionObject().get().get(LABELS));
    }

    public List<ObjectId> getParentIds() {
        return ImmutableList.copyOf((Collection) getVersionObject().get().get(PARENTS));
    }

    public boolean isRoot() {
        return (!isVersioned() || getParentIds().size() == 0);
    }

    public ObjectId getRootId() {
        if (isRoot()) return (ObjectId) getObject().get(ID);
        return getParentIds().get(0);
    }
}
