package com.polycom.analytics.core.apex.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Updates.set;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.commons.collections.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import com.polycom.analytics.core.apex.data.mongo.EnhancedMongoDBConnectable;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil;
import com.polycom.analytics.core.apex.data.mongo.MongoUtil.ObjectIdInfo;

/**
 * TODO: if have multiple MongoDBOutputOperator instances, bulk update may cause update out of order
 * a timestamp field may need to tell what's the request is the latest
 *
 */
public abstract class MongoDBOutputOperator<T> extends EnhancedMongoDBConnectable implements Operator
{
    private static final Logger log = LoggerFactory.getLogger(MongoDBOutputOperator.class);
    protected static final int DEFAULT_BATCH_SIZE = 1000;
    protected static final String DEFAULT_MAX_WINDOW_COLLECTION_NAME = "maxWindow";
    protected static final String DEFAULT_OPERATORID_COLUMN_NAME = "opId";
    protected static final String DEFAULT_WINDOWID_COLUMN_NAME = "windowId";

    /*
     * it defines how many documents are inserted/updated in one batch
     * */
    @Min(1)
    protected int batchSize = DEFAULT_BATCH_SIZE;

    @NotNull
    protected String maxWindowCollectionName = DEFAULT_MAX_WINDOW_COLLECTION_NAME;
    @NotNull
    protected String operatorIdColumnName = DEFAULT_OPERATORID_COLUMN_NAME;
    @NotNull
    protected String windowIdColumnName = DEFAULT_WINDOWID_COLUMN_NAME;

    protected transient long windowId;
    protected transient long lastWindowId;
    protected transient int operatorId;

    public int getBatchSize()
    {
        return batchSize;
    }

    public void setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
    }

    public String getMaxWindowCollectionName()
    {
        return maxWindowCollectionName;
    }

    public void setMaxWindowCollectionName(String maxWindowCollectionName)
    {
        this.maxWindowCollectionName = maxWindowCollectionName;
    }

    public String getOperatorIdColumnName()
    {
        return operatorIdColumnName;
    }

    public void setOperatorIdColumnName(String operatorIdColumnName)
    {
        this.operatorIdColumnName = operatorIdColumnName;
    }

    public String getWindowIdColumnName()
    {
        return windowIdColumnName;
    }

    public void setWindowIdColumnName(String windowIdColumnName)
    {
        this.windowIdColumnName = windowIdColumnName;
    }

    protected transient int tupleId;

    protected transient MongoCollection<Document> maxWindowCollection;
    protected transient List<String> collectionList;
    protected transient Map<String, List<WriteModel<Document>>> collectionToDocListMap;
    protected transient Map<String, WriteModel<Document>> collectionToDocMap;
    protected transient boolean ignoreWindow;

    public abstract void processTuple(T tuple);

    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
    {
        @Override
        public void process(T tuple)
        {
            if (ignoreWindow)
            {
                return;
            }

            try
            {

                processTuple(tuple);
            }
            catch (Exception ex)
            {
                throw new RuntimeException("Exception during process tuple", ex);
            }
        }
    };

    protected ObjectId generateObjectId()
    {
        return new ObjectId(MongoUtil.generateObjectIdStr(new ObjectIdInfo(windowId, operatorId, tupleId)));
    }

    protected void processTupleCommon()
    {
        WriteModel<Document> writeModel;
        for (String col : collectionToDocMap.keySet())
        {
            writeModel = collectionToDocMap.get(col);
            if (writeModel != null)
            {
                List<WriteModel<Document>> writeModelList = collectionToDocListMap.get(col);

                if (writeModelList == null)
                {
                    writeModelList = new LinkedList<>();
                    collectionToDocListMap.put(col, writeModelList);
                }
                writeModelList.add(writeModel);
                if (tupleId % batchSize == 0)
                {
                    doBatchWrite();
                }

                ++tupleId;
            }
        }

    }

    @Override
    public void setup(OperatorContext context)
    {
        collectionList = Lists.newLinkedList();
        collectionToDocListMap = Maps.newHashMap();
        collectionToDocMap = Maps.newHashMap();
        operatorId = context.getId();
        //operatorId = 100;

        connect();
        initLastWindowId();
    }

    private void initLastWindowId()
    {
        maxWindowCollection = mongoDb.getCollection(maxWindowCollectionName);
        FindIterable<Document> itor = maxWindowCollection.find(eq(operatorIdColumnName, operatorId));
        Document curWindowIdDoc = itor.first();
        if (null == curWindowIdDoc)
        {

            Document doc = new Document();
            doc.put(operatorIdColumnName, operatorId);
            doc.put(windowIdColumnName, (long) 0);
            maxWindowCollection.insertOne(doc);
        }
        else
        {
            lastWindowId = curWindowIdDoc.getLong(windowIdColumnName).longValue();

        }
        log.info("lastWindowId is inited: {}", lastWindowId);
    }

    @Override
    public void teardown()
    {
        disconnect();

    }

    @Override
    public void beginWindow(long windowId)
    {
        this.windowId = windowId;
        this.tupleId = 1;
        ignoreWindow = false;
        if (windowId < lastWindowId)
        {
            log.info("ingore current window: {} < lastWindowId: {}", windowId, lastWindowId);
            ignoreWindow = true;
            return;
        }
        else if (windowId == lastWindowId)
        {
            log.info(
                    "current window: {} == lastWindowId: {}, remove documents with same windowId of the operatorId",
                    windowId, lastWindowId);
            StringBuilder low = new StringBuilder();
            StringBuilder high = new StringBuilder();
            MongoUtil.extractLowHighBoundsFromObjectId(new MongoUtil.ObjectIdInfo(windowId, operatorId, 0), low,
                    high);
            Bson docsInLastWindowFilter = and(gte("_id", new ObjectId(low.toString())),
                    lte("_id", new ObjectId(high.toString())));
            for (String collectionName : collectionList)
            {
                mongoDb.getCollection(collectionName).deleteMany(docsInLastWindowFilter);
            }

        }

    }

    @Override
    public void endWindow()
    {
        if (ignoreWindow)
        {
            return;
        }
        doBatchWrite();

    }

    private void doBatchWrite()
    {
        log.info("doBatchWrite+");
        maxWindowCollection.updateOne(eq(operatorIdColumnName, operatorId), set(windowIdColumnName, windowId));

        for (String colName : collectionToDocListMap.keySet())
        {
            List<WriteModel<Document>> writeModels = collectionToDocListMap.get(colName);
            if (CollectionUtils.isNotEmpty(writeModels))
            {
                mongoDb.getCollection(colName).bulkWrite(writeModels);
            }
            writeModels.clear();
        }
    }

}
