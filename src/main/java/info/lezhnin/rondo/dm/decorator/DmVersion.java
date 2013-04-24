package info.lezhnin.rondo.dm.decorator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.sun.istack.internal.Nullable;
import info.lezhnin.rondo.dm.DmObject;
import info.lezhnin.rondo.dm.DmObjectImpl;
import info.lezhnin.rondo.dm.DmPath;
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
public class DmVersion extends DmDocument {
    public static final String VERSION = "rondo_dm.version";
    public static final String LABELS = "labels";
    public static final String PARENTS = "parents";
    public static final String CURRENT_LABEL = "current";
    private static final DmPath versionPath = new DmPath(VERSION);

    public DmVersion(DmObject dmObject) {
        super(dmObject);
    }

    protected Optional<DBObject> getVersionObject() {
        return versionPath.getDBObject(getObject());
    }

    protected DBObject getOrCreateVersionObject() {
        return versionPath.getOrCreateDBObject(getObject());
    }

    @Override
    public DmObject copy() {
        return new DmVersion(super.copy());
    }

    public DmObject create() {
        DmVersion newVersion = new DmVersion(copy());
        DBObject newVersionObject = newVersion.getOrCreateVersionObject();
        List labels = isVersioned() ? getLabels() : new BasicDBList();
        newVersionObject.put(LABELS, labels);
        List parents = isVersioned() ? getParentIds() : new BasicDBList();
        parents.add(0, ((BasicDBObject) getObject()).getObjectId(DmObject.ID));
        newVersionObject.put(PARENTS, parents);
        return newVersion;
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
        if (isRoot()) return ObjectId.massageToObjectId(getObject().get(DmObject.ID));
        List<ObjectId> parentIds = getParentIds();
        return parentIds.get(parentIds.size() - 1);
    }

    public Optional<DmVersion> getVersionWithLabels(Collection<String> labels) {
        BasicDBList labelList = new BasicDBList();
        labelList.addAll(labels);
        DBObject version = getCollection().findOne(new BasicDBObject(VERSION + '.' + LABELS, new BasicDBObject("$in",
                labelList)));
        if (version == null) return Optional.absent();
        return Optional.of(new DmVersion(new DmObjectImpl(version, getCollection())));
    }

    public Optional<DmVersion> getCurrent() {
        return getVersionWithLabels(ImmutableList.of(CURRENT_LABEL));
    }

    public void makeCurrent() {
        Optional<DmVersion> current = getCurrent();
        if (current.isPresent()) {
            current.get().doRemoveLabels(ImmutableList.of(CURRENT_LABEL));
            current.get().save();
        }
        doAddLabels(ImmutableList.of(CURRENT_LABEL));
        save();
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

    public List<ObjectId> getChildIds() {
        BasicDBObject condition = new BasicDBObject(V_P + ".0", getObjectId());
        return getObjectIds(condition);
    }

    public List<ObjectId> getAllChildIds() {
        BasicDBObject condition = new BasicDBObject(V_P, getObjectId());
        return getObjectIds(condition);
    }

    private List<ObjectId> getObjectIds(DBObject condition) {
        Preconditions.checkNotNull(getCollection());
        List<DBObject> children = ImmutableList.copyOf(getCollection().find(condition, new BasicDBObject(DmObject.ID, 1)).iterator());
        return Lists.transform(children, new Function<DBObject, ObjectId>() {
            public ObjectId apply(@Nullable DBObject input) {
                return ObjectId.massageToObjectId(input.get(DmObject.ID));
            }
        });
    }
}
