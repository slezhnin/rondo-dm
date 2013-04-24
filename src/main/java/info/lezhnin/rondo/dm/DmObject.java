package info.lezhnin.rondo.dm;

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
public interface DmObject {
    String ID = "_id";

    public DBObject getObject();

    public DBCollection getCollection();

    public void setCollection(@Nullable DBCollection collection);

    public @Nullable ObjectId getObjectId();

    public void save();

    public boolean load(@Nullable ObjectId objectId);

    public DmObject copy();
}
