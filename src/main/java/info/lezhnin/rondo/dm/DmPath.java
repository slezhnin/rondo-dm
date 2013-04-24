package info.lezhnin.rondo.dm;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import static com.google.common.base.Optional.fromNullable;

/**
 * Description goes here...
 * <p/>
 * Date: 20.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public class DmPath {
    String[] path;

    public DmPath(String path) {
        this.path = path == null ? new String[0] : path.split("\\.");
    }

    public Optional<DBObject> getDBObject(DBObject dbObject) {
        Optional<DBObject> result = fromNullable(dbObject);
        for (String part : path) {
            if (result.isPresent()) {
                Object object = result.get().get(part);
                if (object instanceof DBObject) result = fromNullable((DBObject) object);
                else return Optional.absent();
            }
        }
        return result;
    }

    public DBObject getOrCreateDBObject(DBObject object) {
        Preconditions.checkNotNull(object);
        DBObject result = object;
        for (String part : path) {
            DBObject current = (DBObject) result.get(part);
            if (current == null) {
                current = new BasicDBObject();
                result.put(part, current);
            }
            result = current;
        }
        return result;
    }
}
