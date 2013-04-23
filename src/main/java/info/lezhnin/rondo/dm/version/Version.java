package info.lezhnin.rondo.dm.version;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sun.istack.internal.Nullable;
import info.lezhnin.rondo.dm.Path;
import info.lezhnin.rondo.dm.document.Document;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Description goes here...
 * <p/>
 * Date: 20.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class Version extends Document {
    public static final String VERSION = "rondo_dm.version";
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
        parents.add(0, ((BasicDBObject) getObject()).getObjectId(ID));
        newVersionObject.put(PARENTS, parents);
        return newVersion.getObject();
    }

    public boolean isVersioned() {
        Optional<DBObject> version = getVersionObject();
        return version.isPresent() && version.get().containsField(LABELS) && version.get().containsField(PARENTS);
    }

    public List<String> getLabels() {
        Optional<DBObject> version = getVersionObject();
        Object labels = version.isPresent() ? version.get().get(LABELS) : null;
        if (labels == null) return ImmutableList.of();
        return ImmutableList.copyOf((Iterable<? extends String>) labels);
    }

    public boolean addLabels(Collection<String> newLabels) {
        Preconditions.checkNotNull(newLabels);
        Preconditions.checkArgument(!newLabels.contains(CURRENT_LABEL), "Don't add \"%s\" label manually.",
                CURRENT_LABEL);
        return doAddLabels(newLabels);
    }

    public boolean removeLabels(Collection<String> oldLabels) {
        Preconditions.checkNotNull(oldLabels);
        Preconditions.checkArgument(!oldLabels.contains(CURRENT_LABEL), "Don't remove \"%s\" label manually.",
                CURRENT_LABEL);
        return doRemoveLabels(oldLabels);
    }

    boolean doAddLabels(Collection<String> newLabels) {
        if (newLabels.size() > 0) {
            Set<String> labels = Sets.newHashSet(getLabels());
            if (labels.addAll(newLabels)) {
                BasicDBList labelList = new BasicDBList();
                labelList.addAll(labels);
                getOrCreateVersionObject().put(LABELS, labels);
                return true;
            }
        }
        return false;
    }

    boolean doRemoveLabels(Collection<String> oldLabels) {
        if (oldLabels.size() > 0) {
            Set<String> labels = Sets.newHashSet(getLabels());
            if (labels.removeAll(oldLabels)) {
                BasicDBList labelList = new BasicDBList();
                labelList.addAll(labels);
                getOrCreateVersionObject().put(LABELS, labels);
                return true;
            }
        }
        return false;
    }

    public List<ObjectId> getParentIds() {
        Optional<DBObject> version = getVersionObject();
        Object parents = version.isPresent() ? version.get().get(PARENTS) : null;
        if (parents == null) return ImmutableList.of();
        return ImmutableList.copyOf((Iterable<? extends ObjectId>) parents);
    }

    public boolean isRoot() {
        return (!isVersioned() || getParentIds().size() == 0);
    }

    public ObjectId getRootId() {
        if (isRoot()) return ObjectId.massageToObjectId(getObject().get(ID));
        List<ObjectId> parentIds = getParentIds();
        return parentIds.get(parentIds.size() - 1);
    }

    public Optional<Version> getVersionWithLabels(DBCollection collection, Collection<String> labels) {
        BasicDBList labelList = new BasicDBList();
        labelList.addAll(labels);
        DBObject version = collection.findOne(new BasicDBObject(VERSION + '.' + LABELS, new BasicDBObject("$in",
                labelList)));
        if (version == null) return Optional.absent();
        return Optional.of(new Version(version));
    }

    public Optional<Version> getCurrent(DBCollection collection) {
        return getVersionWithLabels(collection, ImmutableList.of(CURRENT_LABEL));
    }

    public void makeCurrent(DBCollection collection) {
        Optional<Version> current = getCurrent(collection);
        if (current.isPresent()) {
            current.get().doRemoveLabels(ImmutableList.of(CURRENT_LABEL));
            current.get().save(collection);
        }
        doAddLabels(ImmutableList.of(CURRENT_LABEL));
        save(collection);
    }

    public boolean isCurrent() {
        return getLabels().contains(CURRENT_LABEL);
    }

    public Optional<ObjectId> getParentId() {
        List<ObjectId> parents = getParentIds();
        if (parents.size() == 0) return Optional.absent();
        return Optional.of(parents.get(0));
    }

    private static final String V_P = VERSION + '.' + PARENTS;

    public List<ObjectId> getChildIds(DBCollection collection) {
        BasicDBObject condition = new BasicDBObject(V_P + ".0", getObjectId());
        return getObjectIds(collection, condition);
    }

    public List<ObjectId> getAllChildIds(DBCollection collection) {
        BasicDBObject condition = new BasicDBObject(V_P, getObjectId());
        return getObjectIds(collection, condition);
    }

    private List<ObjectId> getObjectIds(DBCollection collection, BasicDBObject condition) {
        Preconditions.checkNotNull(collection);
        List<DBObject> children = ImmutableList.copyOf(collection.find(condition, new BasicDBObject(ID, 1)).iterator());
        return Lists.transform(children, new Function<DBObject, ObjectId>() {
            public ObjectId apply(@Nullable DBObject input) {
                return ObjectId.massageToObjectId(input.get(ID));
            }
        });
    }
}
