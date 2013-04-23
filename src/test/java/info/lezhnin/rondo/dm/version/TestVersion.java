package info.lezhnin.rondo.dm.version;

import com.google.common.collect.ImmutableList;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;
import info.lezhnin.rondo.dm.MongoDB;
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

    private DB database;
    private DBCollection collection;
    private boolean databaseEnabled = false;

    @Before
    public void before() {
        try {
            database = MongoDB.INSTANCE.getDatabase();
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
        Version version = new Version();
        version.getObject().put("_id", new ObjectId());
        LOGGER.debug("document: {}", version);

        Version newVersion = new Version(version.create());
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
        version.doAddLabels(ImmutableList.of(Version.CURRENT_LABEL));
        assertTrue(version.isCurrent());
        version.doRemoveLabels(ImmutableList.of(Version.CURRENT_LABEL));
        assertFalse(version.isCurrent());

        if (databaseEnabled) testWithDatabase();
    }

    private void testWithDatabase() {
        LOGGER.debug("Test with database...");

        Version version = new Version();
        version.save(collection);
        LOGGER.debug("document: {}", version);

        Version newVersion = new Version(version.create());
        newVersion.save(collection);
        LOGGER.debug("new version: {}", newVersion);

        List parents = newVersion.getParentIds();

        assertTrue(version.isRoot());
        assertTrue(parents.contains(version.getObjectId()));
        assertEquals(version.getObjectId(), version.getRootId());
        assertEquals(version.getObjectId(), newVersion.getRootId());

        assertFalse(version.getCurrent(collection).isPresent());
        version.makeCurrent(collection);
        LOGGER.debug("version is current: {}", version);
        assertTrue(version.getCurrent(collection).isPresent());
        assertTrue(version.isCurrent());

        newVersion.makeCurrent(collection);
        LOGGER.debug("new version is current: {}", newVersion);
        assertTrue(version.getCurrent(collection).isPresent());
        assertTrue(newVersion.isCurrent());
        version.load(collection);
        LOGGER.debug("old version is not current: {}", version);
        assertFalse(version.isCurrent());

        assertTrue(newVersion.getParentId().get().equals(version.getObjectId()));
        assertFalse(version.getParentId().isPresent());
        assertEquals(ImmutableList.of(), newVersion.getAllChildIds(collection));
        assertEquals(ImmutableList.of(), newVersion.getChildIds(collection));
        assertEquals(ImmutableList.of(newVersion.getObjectId()), version.getAllChildIds(collection));
        assertEquals(ImmutableList.of(newVersion.getObjectId()), version.getChildIds(collection));
    }
}
