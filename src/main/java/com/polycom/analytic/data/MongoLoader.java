package com.polycom.analytic.data;

import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datatorrent.contrib.enrich.BackendLoader;
import com.datatorrent.lib.util.FieldInfo;

public class MongoLoader extends JongoMongoDBConnectable implements BackendLoader
{

    private static final Logger log = LoggerFactory.getLogger(MongoLoader.class);

    @Override
    public Map<Object, Object> loadInitialData()
    {
        return null;
    }

    @Override
    public Object get(Object key)
    {

        Criteria criteria = (Criteria) key;
        String tableName = criteria.getTableName();
        MongoCollection col = jongo.getCollection(tableName);
        if (criteria.isConditionTemplate())
        {
            return col.findOne(criteria.getCondition(), criteria.getParas()).as(criteria.getReturnType());
        }
        return col.findOne(criteria.getCondition()).as(criteria.getReturnType());

    }

    @Override
    public List<Object> getAll(List<Object> keys)
    {
        // TODO Implement getAll
        throw new UnsupportedOperationException("getAll Not Implemented");

    }

    @Override
    public void put(Object key, Object value)
    {
        // TODO Implement put
        throw new UnsupportedOperationException("put Not Implemented");

    }

    @Override
    public void putAll(Map<Object, Object> m)
    {
        // TODO Implement putAll
        throw new UnsupportedOperationException("putAll Not Implemented");

    }

    @Override
    public void remove(Object key)
    {
        // TODO Implement remove
        throw new UnsupportedOperationException("remove Not Implemented");

    }

    public static void main(
            String[] args) throws UnknownHostException, NoSuchAlgorithmException, KeyManagementException
    {
        String userName = "dbadmin";
        String password = "kaiCoon8yo";
        String hostName = "192.168.60.24:27017";

        /* SSLContext ssl = SSLContext.getInstance("SSL");
        TrustManager[] trust = new TrustManager[] { new X509TrustManager()
        {
        
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException
            {
        
            }
        
            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException
            {
        
            }
        
            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return null;
        
            }
        
        } };
        ssl.init(null, trust, new java.security.SecureRandom());
        SSLSocketFactory sss = ssl.getSocketFactory();
        MongoClient mongoClient = new MongoClient(
                Arrays.asList(new ServerAddress("192.168.60.24", 27017), new ServerAddress("192.168.60.25", 27017),
                        new ServerAddress("192.168.60.26", 27017)),
                MongoCredential.createCredential(userName, "admin", password.toCharArray()),
                MongoClientOptions.builder().requiredReplicaSetName("hanalyticsrs0")
                        .sslInvalidHostNameAllowed(true).socketFactory(sss).sslEnabled(true).build());
        
        //MongoClientOptions.builder().socketFactory(SSLSocketFactory.getDefault()).build());
        System.out.println(mongoClient.getReplicaSetStatus());
        ListDatabasesIterable<Document> dbs = mongoClient.listDatabases();
        MongoCursor<Document> ditor = dbs.iterator();
        while (ditor.hasNext())
        {
            System.out.println(ditor.next());
        }*/

        // System.out.println(mongoClient.getDatabaseNames());
        /* MongoDatabase ruleDb = mongoClient.getDatabase("rule_config");
        com.mongodb.client.MongoCollection<Document> collection = ruleDb.getCollection("test");
        Document document = new Document("title", "MongoDB").append("description", "database").append("likes", 100)
                .append("by", "Fly");
        List<Document> documents = new ArrayList<Document>();
        documents.add(document);
        collection.insertMany(documents);
        System.out.println(mongoClient.getDatabaseNames());
        System.out.println(collection.count());
        ruleDb.drop();
        System.out.println(mongoClient.getDatabaseNames());*/

        /*db = mongoClient.getDB(dataBase);
        if (userName != null && passWord != null) {
          db.authenticate(userName, passWord.toCharArray());
        }*/

        MongoLoader loader = new MongoLoader();

        // loader.setHostName("172.21.120.143:27017");
        loader.setDataBase("rule_config");
        loader.setHostName("hanalyticsrs0/192.168.60.24:27017,192.168.60.25:27017,192.168.60.26:27017");
        loader.setAuthDB("admin");
        loader.setUserName(userName);
        loader.setPassWord(password);
        System.out.println("begin.....");
        loader.connect();
        System.out.println(loader.isConnected());
        /*    loader.disconnect();
        EnhancedMongoDBConnectable con = new EnhancedMongoDBConnectable();
        con.setDataBase("rule_config");
        con.setHostName("172.21.120.143:27017");
        con.connect();
        System.out.println(con.isConnected());
        con.disconnect();*/
        Criteria c = new Criteria();
        c.setTableName("customer_4254d035-f1f0-45b4-9a9c-013f9099235a");

        c.setParas(new Object[] { "9384b5bf-52a1-40f0-8faa-83f9d82c49fd" });
        c.setReturnType(JSONObject.class);
        c.setCondition("{tenantID:#}");

        System.out.println("v: " + c.isValid());
        JSONObject j = JSONObject.class.cast(loader.get(c));
        System.out.println(j);
        JSONArray jar = j.getJSONArray("rules");

        loader.disconnect();
        /*Jongo jongo = loader.getJongo();
        MongoCollection customer_rule = jongo.getCollection("customer");
        JSONObject r = customer_rule.findOne("{tenantID:#}", "9384b5bf-52a1-40f0-8faa-83f9d82c49fd")
                .as(JSONObject.class);
        System.out.println(r);
        System.out.println(r.get("rules"));*/
        /*
        while (all.hasNext())
        {
           JSONObject r = all.next();
           System.out.println(r);
           System.out.println(r.get("rules"));
        }*/

    }

    @Override
    public void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo)
    {

    }

}
