package com.polycom.analytic.data.mongo;

import org.jongo.Jongo;

import com.mongodb.DB;

public class JongoMongoDBConnectable extends EnhancedMongoDBConnectable
{
    protected transient Jongo jongo;
    protected transient DB db;

    @SuppressWarnings("deprecation")
    @Override
    public void connect()
    {
        super.connect();
        db = mongoClient.getDB(dataBase);
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
}
