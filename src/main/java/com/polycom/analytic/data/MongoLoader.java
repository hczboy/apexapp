package com.polycom.analytic.data;

import java.util.List;
import java.util.Map;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datatorrent.contrib.mongodb.MongoDBConnectable;

public class MongoLoader extends MongoDBConnectable implements IBackendLoader
{

    private transient Jongo jongo;

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

    @Override
    public void connect()
    {

        super.connect();

        jongo = new Jongo(db);

    }

    @Override
    public boolean isConnected()
    {
        try
        {
            jongo.runCommand("{'ping':1}");
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }

    public Jongo getJongo()
    {
        return jongo;
    }

    public static void main(String[] args)
    {
        MongoLoader loader = new MongoLoader();
        loader.setDataBase("rule_config");
        loader.setHostName("172.21.120.143");
        System.out.println("begin");
        loader.connect();
        System.out.println(loader.isConnected());
        Criteria c = new Criteria();
        c.setTableName("customer_4254d035-f1f0-45b4-9a9c-013f9099235a");
        c.setParas(new Object[] { "9384b5bf-52a1-40f0-8faa-83f9d82c49fd" });
        c.setReturnType(JSONObject.class);
        c.setCondition("{tenantID:#}");
        JSONObject j = JSONObject.class.cast(loader.get(c));
        System.out.println(j);
        JSONArray jar = j.getJSONArray("rules");
        int s = jar.size();
        for (int i = 0; i < s; i++)
        {
            JSONObject r = jar.getJSONObject(i);

            System.out.println(r.getObject("def", Map.Entry.class));
            System.out.println(r.getJSONArray("commands"));
        }
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
        loader.disconnect();

    }

}
