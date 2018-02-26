package com.polycom.analytic.data.mongo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class MongoUtil
{
    private MongoUtil()
    {
    }

    public static class ObjectIdInfo
    {
        final long windowId;
        final int operatorId;
        final int tupleId;

        public ObjectIdInfo(long windowId, int operatorId, int tupleId)
        {
            this.windowId = windowId;
            this.operatorId = operatorId;
            this.tupleId = tupleId;
        }
    }

    /**
     * 8B windowId | 1B operatorId | 3B tupleId
     */
    public static String generateObjectIdStr(ObjectIdInfo objIdInfo)
    {
        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putLong(objIdInfo.windowId);
        byte oid = (byte) (objIdInfo.operatorId);
        bb.put(oid);
        for (int i = 0; i < 3; i++)
        {
            byte num = (byte) (objIdInfo.tupleId >> 8 * (2 - i));
            bb.put(num);
        }
        StringBuilder objStr = new StringBuilder();
        for (byte b : bb.array())
        {
            objStr.append(String.format("%02x", b & 0xff));
            // System.out.println(String.format("%02x", b & 0xff));
        }
        return objStr.toString();
    }

    public static void extractLowHighBoundsFromObjectId(ObjectIdInfo objIdInfo, StringBuilder low,
            StringBuilder high)
    {
        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.order(ByteOrder.BIG_ENDIAN);
        bb.putLong(objIdInfo.windowId);
        byte opId = (byte) (objIdInfo.operatorId);
        bb.put(opId);
        ByteBuffer lowbb = bb;
        lowbb.mark();
        lowbb.put((byte) 0);
        lowbb.put((byte) 0);
        lowbb.put((byte) 0);
        for (byte b : bb.array())
        {
            low.append(String.format("%02x", b & 0xff));
        }
        ByteBuffer highbb = bb;
        highbb.reset();
        highbb.put((byte) 0xff);
        highbb.put((byte) 0xff);
        highbb.put((byte) 0xff);
        for (byte b : highbb.array())
        {
            high.append(String.format("%02x", b & 0xff));
        }
    }

    /*  public static void main(String[] args)
    {
        System.out.println(generateObjectIdStr(new ObjectIdInfo(1, 2, 4)));
        StringBuilder low = new StringBuilder();
        StringBuilder high = new StringBuilder();
        extractLowHighBoundsFromObjectId(new ObjectIdInfo(1, 2, 4), low, high);
        System.out.println(low);
        System.out.println(high);
    }*/

}
