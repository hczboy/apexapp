package com.polycom.analytics.core.apex.data.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections.CollectionUtils;
import org.jongo.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.datatorrent.contrib.enrich.BackendLoader;
import com.datatorrent.lib.util.FieldInfo;
import com.google.common.collect.Lists;

public class BasicMongoLoader extends JongoMongoDBConnectable implements BackendLoader
{

    private static final Logger log = LoggerFactory.getLogger(BasicMongoLoader.class);

    @NotNull
    protected String collectionName;

    protected transient List<FieldInfo> includeFieldInfo;

    protected transient List<FieldInfo> lookupFieldInfo;

    public String getCollectionName()
    {
        return collectionName;
    }

    public void setCollectionName(String collectionName)
    {
        this.collectionName = collectionName;
    }

    public List<FieldInfo> getIncludeFieldInfo()
    {
        return includeFieldInfo;
    }

    public void setIncludeFieldInfo(List<FieldInfo> includeFieldInfo)
    {
        this.includeFieldInfo = includeFieldInfo;
    }

    public List<FieldInfo> getLookupFieldInfo()
    {
        return lookupFieldInfo;
    }

    public void setLookupFieldInfo(List<FieldInfo> lookupFieldInfo)
    {
        this.lookupFieldInfo = lookupFieldInfo;
    }

    protected String query;
    protected String projection;

    @Override
    public Map<Object, Object> loadInitialData()
    {
        return Collections.EMPTY_MAP;

    }

    @Override
    public Object get(Object key)
    {
        ArrayList<Object> queryParams = (ArrayList<Object>) key;
        MongoCollection col = jongo.getCollection(collectionName);
        /*
         * original intent is to remove usage of lib fastjson, then try JsonNode here first
         * But when downstream operator deserializes JsonNode, it would report error like
         *  com.esotericsoftware.kryo.KryoException: Class cannot be created (missing no-arg constructor): com.fasterxml.jackson.databind.node.ObjectNode
        at com.esotericsoftware.kryo.Kryo$DefaultInstantiatorStrategy.newInstantiatorOf(Kryo.java:1228)
        at com.esotericsoftware.kryo.Kryo.newInstantiator(Kryo.java:1049)
        at com.esotericsoftware.kryo.Kryo.newInstance(Kryo.java:1058)
        at com.esotericsoftware.kryo.serializers.FieldSerializer.create(FieldSerializer.java:547)
        at com.esotericsoftware.kryo.serializers.FieldSerializer.read(FieldSerializer.java:523)
        at com.esotericsoftware.kryo.Kryo.readClassAndObject(Kryo.java:761)
        at com.esotericsoftware.kryo.serializers.MapSerializer.read(MapSerializer.java:143)
        at com.esotericsoftware.kryo.serializers.MapSerializer.read(MapSerializer.java:21)
        at com.esotericsoftware.kryo.Kryo.readClassAndObject(Kryo.java:761)
        at com.datatorrent.stram.codec.DefaultStatefulStreamCodec.fromDataStatePair(DefaultStatefulStreamCodec.java:98)
        at com.datatorrent.stram.stream.BufferServerSubscriber$BufferReservoir.processPayload(BufferServerSubscriber.java:391)
        at com.datatorrent.stram.stream.BufferServerSubscriber$BufferReservoir.sweep(BufferServerSubscriber.java:339)
        at com.datatorrent.stram.engine.WindowIdActivatedReservoir.sweep(WindowIdActivatedReservoir.java:92)
        at com.datatorrent.stram.engine.GenericNode.run(GenericNode.java:269)
        at com.datatorrent.stram.engine.StreamingContainer$2.run(StreamingContainer.java:1428)
        *As such, have to switch to JSONObject coming from fastjson
         * */
        JSONObject ret = col.findOne(query, queryParams.toArray()).projection(projection).as(JSONObject.class);
        if (ret == null)
        {
            return null;
        }
        ArrayList<Object> retList = Lists.newArrayListWithCapacity(includeFieldInfo.size());
        for (int i = 0; i < includeFieldInfo.size(); i++)
        {
            retList.add(ret.get(includeFieldInfo.get(i).getColumnName()));
        }
        return retList;
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
    public void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo)
    {
        this.lookupFieldInfo = lookupFieldInfo;
        this.includeFieldInfo = includeFieldInfo;
        query = generateQueryTemplate(lookupFieldInfo);
        projection = generateProjection(includeFieldInfo, true);
        log.info("setFieldInfo+: generate query:{}, projection: {}", query, projection);
    }

    protected String generateQueryTemplate(List<FieldInfo> lookupFieldInfos)
    {
        if (CollectionUtils.isEmpty(lookupFieldInfos))
        {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < lookupFieldInfos.size(); i++)
        {
            sb.append(lookupFieldInfos.get(i).getColumnName()).append(":#");
            if (i == lookupFieldInfos.size() - 1)
            {
                sb.append("}");
            }
            else
            {
                sb.append(",");
            }

        }
        return sb.toString();
    }

    protected String generateProjection(List<FieldInfo> includeFieldInfos, boolean excludeId)
    {
        if (CollectionUtils.isEmpty(includeFieldInfos))
        {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < includeFieldInfos.size(); i++)
        {
            sb.append(includeFieldInfos.get(i).getColumnName()).append(":1");
            if (i == includeFieldInfos.size() - 1)
            {
                if (excludeId)
                {
                    sb.append(",").append("_id:0");
                }
                sb.append("}");
            }
            else
            {
                sb.append(",");
            }

        }
        return sb.toString();
    }

}
