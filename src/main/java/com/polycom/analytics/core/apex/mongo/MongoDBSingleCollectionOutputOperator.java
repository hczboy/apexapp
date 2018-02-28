package com.polycom.analytics.core.apex.mongo;

import javax.validation.constraints.NotNull;

import org.bson.Document;

import com.datatorrent.api.Context.OperatorContext;
import com.mongodb.client.model.WriteModel;

public abstract class MongoDBSingleCollectionOutputOperator<T> extends MongoDBOutputOperator<T>
{
    @NotNull
    protected String collectionName;

    public String getCollectionName()
    {
        return collectionName;
    }

    public void setCollectionName(String collectionName)
    {
        this.collectionName = collectionName;

    }

    @Override
    public void setup(OperatorContext context)
    {
        super.setup(context);
        collectionList.add(collectionName);

    }

    @Override
    public void processTuple(T tuple)
    {
        collectionToDocMap.clear();
        WriteModel<Document> writeModel = convertWriteModel(tuple);
        if (writeModel != null)
        {
            collectionToDocMap.put(collectionName, writeModel);
            processTupleCommon();
        }
    }

    protected abstract WriteModel<Document> convertWriteModel(T tuple);

}
