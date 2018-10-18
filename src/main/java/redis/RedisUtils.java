package redis;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.UUID;

public class RedisUtils {

    /**
     * 保存每个线程独有的token
     */
    private static ThreadLocal<String> tokenMap = new ThreadLocal<>();

    private static JedisPool jedisPool = new JedisPool();

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * redis实现分布式可重入锁,并不保证在过期时间内完成锁定内的任务，需根据业务逻辑合理分配seconds
     *
     * @param lock
     *            锁的名称
     * @param seconds
     *            锁定时间，单位 秒
     *  token
     *            对于同一个lock,相同的token可以再次获取该锁，不相同的token线程需等待到unlock之后才能获取
     * @return
     */
    public boolean lock(final String lock,final  int seconds) {
        // @param token 对于同一个lock,相同的token可以再次获取该锁，不相同的token线程需等待到unlock之后才能获取
        String token = tokenMap.get();
        if (StringUtils.isEmpty(token)) {
            token = UUID.randomUUID().toString();
            tokenMap.set(token);
        }
        boolean flag = false;
        Jedis client = null;
        try {
            client = jedisPool.getResource();
            /**
             * lock 锁
             * token 客户端标识（线程标识） 一举两得
             * NX : set if not exist
             * PX : expx 过期设置
             * seconds 过期时间
             */
            String ret = client.set(lock, token, "NX", "EX", seconds);
            if (ret == null) {// 该lock的锁已经存在
                String origToken = client.get(lock);
                if (token.equals(origToken) || origToken == null) {// token相同默认为同一线程，所以token应该尽量长且随机，保证不同线程的该值不相同
                    ret = client.set(lock, token, "NX", "EX", seconds);//
                    if ("OK".equalsIgnoreCase(ret)){
                        flag = true;
                    }
                }
            } else if ("OK".equalsIgnoreCase(ret)){
                flag = true;
            }
        } catch (Exception e) {
            logger.error(" lock{}  失败");
            throw new RedisException(e);
        } finally {
            if (client != null)
                client.close();
        }
        return flag;
    }
    public void lock2(final String lock,final  int seconds,String token){
        Jedis client = null;
        try {
            client = jedisPool.getResource();
            String ret = client.set(lock, token, "NX", "EX", seconds);
            if (ret == null) {

            }
        } catch (Exception e) {
            logger.error(" lock{}  失败");
            throw new RedisException(e);
        } finally {
            if (client != null)
                client.close();
        }
    }
    /**
     * redis可以保证lua中的键的原子操作
     * unlock:lock调用完之后需unlock,否则需等待lock自动过期
     * @param lock
     *  token 只有线程已经获取了该锁才能释放它（token相同表示已获取）
     *
     */
    public void unlock(final String lock) {
        Jedis client = null;
        final String token = tokenMap.get();
        if (StringUtils.isEmpty(token))
            return;
        try {
            client = jedisPool.getResource();
            final String script = "if redis.call(\"get\",\"" + lock + "\") == \"" + token + "\"then  return redis.call(\"del\",\"" + lock + "\") else return 0 end ";
            client.eval(script);
        } catch (Exception e) {
            logger.error(" unlock{}  失败");
            throw new RedisException(e);
        } finally {
            if (client != null)
                client.close();
        }

    }

    /**2.6以前版本
     * setnx实现加锁方式一
     * @param key
     * @param value
     */
    public boolean tryLock1(String key,String value){
        Jedis jedis = jedisPool.getResource();
        Long ret = jedis.setnx(key,value);
        if (ret == 1)
            return true;

        //加锁失败,再次进入抢锁逻辑
        String oldExpireTimeStr = jedis.get(key);
        if ( oldExpireTimeStr!= null && Long.parseLong(oldExpireTimeStr) < now() ){
            //说明锁过期,设置锁的新的过期时间，返回旧的过期时间
            String oldValueStr = jedis.getSet(key, now()+1+"");//会被其他的线程重置过期时间的风险
            if (oldValueStr != null && oldValueStr.equals(oldExpireTimeStr)){
                //获得锁
                return true;
            }
        }
        return false;
    }

    /**
     * 2.6以前版本
     * setnx实现加锁
     * @param key
     * @param value
     * @return
     */
    public boolean tryLock2(String key,String value){
        Jedis jedis = jedisPool.getResource();
        Long ret = jedis.setnx(key,value);
        if (ret == 1){
            //1、如果在此之前机器宕机或其他故障，则造成了死锁
            //采用lua设置过期时间如何
            jedis.expire(key,1);
        }
        return false;
    }


    /**
     * 解锁方式错误1
     * @param key
     * @param clientId
     */
    public void relaseLock1(String key,String clientId){
        Jedis jedis = jedisPool.getResource();
        //下面的操作不是原子性的
        if (clientId.equals(jedis.get(key))){

            //此时这把锁突然不是这个客户端的，误解锁
            jedis.del(key);
        }
    }

    public long now(){
        return System.currentTimeMillis()/1000;
    }

    public static void main(String[] args) {
//        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("applicationContext.xml");
//        final RedisUtils redis = ctx.getBean(RedisUtils.class);
        final RedisUtils redis = new RedisUtils();
        for (int i = 0; i < 100; i++) {
            final int index = i;
            new Thread(new Runnable() {
                String key = "huobi";
                @Override
                public void run() {
                    boolean lock = redis.lock(key, 1);
                    System.out.println(key+index+"加锁"+lock + "-");
                    if (lock){
                        redis.unlock(key);
                        System.out.println(key+index+"释放锁"+"成功" + "-");
                    }

                }
            }).start();

        }

        // ctx.close();
    }
}
