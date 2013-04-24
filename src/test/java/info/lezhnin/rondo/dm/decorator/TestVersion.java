package info.lezhnin.rondo.dm.decorator;

import com.google.common.collect.ImmutableList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import info.lezhnin.rondo.dm.DmMongoDB;
import info.lezhnin.rondo.dm.DmObjectImpl;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Description goes here...
 * <p/>
 * Date: 20.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class TestVersion {
    static final Logger LOGGER = LoggerFactory.getLogger(TestVersion.class);
    public static final String COLLECTION_NAME = "documents";

    private DB database = null;
    private DBCollection collection = null;
    private boolean databaseEnabled = false;

    @Before
    public void before() {
        try {
            database = DmMongoDB.INSTANCE.getDatabase();
            collection = database.getCollection(COLLECTION_NAME);
            databaseEnabled = true;
        } catch (MongoException me) {
            databaseEnabled = false;
            LOGGER.warn("Connection error: {}", me);
        }
    }

    @After
    public void after() {
        database.dropDatabase();
    }

    @Test
    public void test() {
        DmVersion version = new DmVersion(new DmObjectImpl());
        version.getObject().put("_id", new ObjectId());
        LOGGER.debug("document: {}", version);

        DmVersion newVersion = (DmVersion) version.create();
        LOGGER.debug("new version: {}", newVersion);

        List parents = newVersion.getParentIds();

        assertTrue(version.isRoot());
        assertTrue(parents.contains(version.getObjectId()));
        assertEquals(version.getObjectId(), version.getRootId());
        assertEquals(version.getObjectId(), newVersion.getRootId());
        assertFalse(version.isCurrent());
        assertTrue(newVersion.getParentId().get().equals(version.getObjectId()));
        assertFalse(version.getParentId().isPresent());

        // It's now checked not to add/remove "current" label manually in addLabels/removeLabels.
        version.doAddLabels(ImmutableList.of(DmVersion.CURRENT_LABEL));
        assertTrue(version.isCurrent());
        version.doRemoveLabels(ImmutableList.of(DmVersion.CURRENT_LABEL));
        assertFalse(version.isCurrent());

        if (databaseEnabled) testWithDatabase();
    }

    private void testWithDatabase() {
        LOGGER.debug("Test with database...");

        DmVersion version = new DmVersion(new DmObjectImpl(collection));
        version.save();
        LOGGER.debug("document: {}", version);

        DmVersion newVersion = (DmVersion) version.create();
        newVersion.save();
        LOGGER.debug("new version: {}", newVersion);

        List parents = newVersion.getParentIds();

        assertTrue(version.isRoot());
        assertTrue(parents.contains(version.getObjectId()));
        assertEquals(version.getObjectId(), version.getRootId());
        assertEquals(version.getObjectId(), newVersion.getRootId());

        assertFalse(version.getCurrent().isPresent());
        version.makeCurrent();
        LOGGER.debug("version is current: {}", version);
        assertTrue(version.getCurrent().isPresent());
        assertTrue(version.isCurrent());

        newVersion.makeCurrent();
        LOGGER.debug("new version is current: {}", newVersion);
        assertTrue(version.getCurrent().isPresent());
        assertTrue(newVersion.isCurrent());
        version.load(null);
        LOGGER.debug("old version is not current: {}", version);
        assertFalse(version.isCurrent());

        assertTrue(newVersion.getParentId().get().equals(version.getObjectId()));
        assertFalse(version.getParentId().isPresent());
        assertEquals(ImmutableList.of(), newVersion.getAllChildIds());
        assertEquals(ImmutableList.of(), newVersion.getChildIds());
        assertEquals(ImmutableList.of(newVersion.getObjectId()), version.getAllChildIds());
        assertEquals(ImmutableList.of(newVersion.getObjectId()), version.getChildIds());
    }
}
