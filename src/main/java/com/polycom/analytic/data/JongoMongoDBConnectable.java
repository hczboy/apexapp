package com.polycom.analytic.data;

import org.jongo.Jongo;

public class JongoMongoDBConnectable extends EnhancedMongoDBConnectable
{
    protected transient Jongo jongo;

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
