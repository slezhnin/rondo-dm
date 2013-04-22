package info.lezhnin.rondo.dm.version;

import com.mongodb.*;
import info.lezhnin.rondo.dm.MongoDB;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    }
}
