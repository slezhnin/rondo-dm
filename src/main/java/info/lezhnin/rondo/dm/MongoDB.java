package info.lezhnin.rondo.dm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Description goes here...
 * <p/>
 * Date: 22.04.13
 *
 * @author Sergey Lezhnin <s.lezhnin@gmail.com>
 */
public enum MongoDB {
    INSTANCE;

    static final Logger LOGGER = LoggerFactory.getLogger(MongoDB.class);
    static final String MONGODB_YAML = "mongodb.yaml";
    static final String CONNECTION = "connection";
    static final String HOST = "host";
    static final String PORT = "port";
    static final String DATABASE = "database";

    Map<String, Object> getConfig() {
        Yaml yaml = new Yaml();
        InputStream resource = getClass().getClassLoader().getResourceAsStream(MONGODB_YAML);
        Preconditions.checkNotNull(resource);
        return (Map<String, Object>) yaml.load(resource);
    }

    public MongoClient getClient() {
        Map<String, Object> config = getConfig();
        List<Map<String, Object>> connections = (List<Map<String, Object>>) config.get(CONNECTION);
        List<ServerAddress> addressList = Lists.newArrayList();
        for (Map<String, Object> address : connections) {
            try {
                addressList.add(new ServerAddress((String) address.get(HOST), (Integer) address.get(PORT)));
            } catch (UnknownHostException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return new MongoClient(addressList);
    }

    public DB getDatabase() {
        Map<String, Object> config = getConfig();
        return getClient().getDB((String) config.get(DATABASE));
    }
}
