package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lijingxiao on 2018/10/25.
 */
public class JedisConnectionPool {
    private static final Logger LOG = LoggerFactory.getLogger(JedisConnectionPool.class);

    public static final int MAX_ACTIVE = 10;
    public static final int MAX_IDLE = 4 ;
    public static final int TIME_OUT = 20000;
    public static final int RETRY_NUM = 3;
    /**
     * 保存多个连接源
     */
    private static Map<String, JedisPool> poolMap = new HashMap<String, JedisPool>();

    public static void main(String[] args) {
        JedisPool jedisPool = getPool();
        Jedis resource = jedisPool.getResource();
        resource.set("income", "10000");
        String r1 = resource.get("income");

        Long r2 = resource.incrBy("income", 100);
        System.out.println(r1 + ", " + r2.toString());
    }

    public static JedisPool getPool() {
        JedisPool pool = null;
        try {
            String key = "JedisDemo";
            if (!poolMap.containsKey(key)) {
                JedisPoolConfig config = new JedisPoolConfig();
                //最大连接数
                config.setMaxTotal(MAX_ACTIVE);
                //最大空闲连接数
                config.setMaxIdle(MAX_IDLE);
                //当调用borrow Object方法时，是否进行有效性检查
                config.setTestOnBorrow(true);
                int timeOut = JedisConnectionPool.TIME_OUT;
                pool = new JedisPool(config, "127.0.0.1", 6379, timeOut);
                poolMap.put(key, pool);
            } else {
                pool = poolMap.get(key);
            }
        } catch (Exception e) {
            LOG.error("init jedis pool failed ! " + e.getMessage(), e);
        }
        return null;
    }
}
