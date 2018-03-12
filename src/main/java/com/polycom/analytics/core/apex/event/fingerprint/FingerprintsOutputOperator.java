package com.polycom.analytics.core.apex.event.fingerprint;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.setOnInsert;
import static com.polycom.analytics.core.apex.common.Constants.ATTACHEDDEVICE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICECONFIGRECORD_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINT_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.INFOTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.NETWORKINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.PRIMARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SECONDARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.polycom.analytics.core.apex.mongo.MongoDBSingleCollectionOutputOperator;
import com.polycom.analytics.core.apex.util.JsonUtil;
import com.polycom.analytics.core.apex.util.KeyValWrapper;

public class FingerprintsOutputOperator extends MongoDBSingleCollectionOutputOperator<Map<String, Object>>
{

    private static final Logger log = LoggerFactory.getLogger(FingerprintsOutputOperator.class);
    /*    private static final String DEVICEID_NAME_IN_COL = "deviceID";
    private static final String SERIALNUMBER_NAME_IN_COL = "sn";
    private static final String PRIMARY_DEVICE_INFO_NAME_IN_COL = "primary";
    private static final String SECONDARY_DEVICE_INFO_NAME_IN_COL = "second";
    private static final String NETWORK_INFO_NAME_IN_COL = "network";
    private static final String DEVICE_CONFIG_RECORD_NAME_IN_COL = "deviceConfigRec";*/

    private static final Set<String> REQUIRE_FIELD_SET = new HashSet<>(
            Arrays.asList(DEVICEID_FIELD, SERIALNUMBER_FIELD, INFOTYPE_FIELD));

    private static final Set<String> VALID_INFOTYPE_VALUE_SET = new HashSet<>(Arrays.asList(
            PRIMARYDEVICEINFO_FIELD, SECONDARYDEVICEINFO_FIELD, NETWORKINFO_FIELD, DEVICECONFIGRECORD_FIELD));

    private WriteModel<Document> generateWriteModel(Map<String, Object> tuple)
    {

        Bson eqDeviceID = eq(DEVICEID_FIELD, tuple.get(DEVICEID_FIELD));
        Bson eqSerialNum = eq(SERIALNUMBER_FIELD, tuple.get(SERIALNUMBER_FIELD));
        String infoType = null;
        for (String infoT : VALID_INFOTYPE_VALUE_SET)
        {
            if (infoT.equals(tuple.get(INFOTYPE_FIELD)))
            {
                infoType = infoT;
                break;
            }
        }

        if (infoType == null)
        {
            log.error("field infoType: {} is invalid", tuple.get(INFOTYPE_FIELD));
            return null;
        }

        Bson query = and(eqDeviceID, eqSerialNum);

        Bson updateFingerPrint = generateUpdateFingerPrint(infoType, tuple);
        if (updateFingerPrint == null)
        {
            log.error("failed to generateUpdateFingerPrint for infoType: {}", infoType);
            return null;
        }
        Bson setOnInsertId = setOnInsert("_id", generateObjectId());
        Bson update = combine(updateFingerPrint, setOnInsertId);

        UpdateOneModel<Document> updateModel = new UpdateOneModel<>(query, update,
                new UpdateOptions().upsert(true));
        return updateModel;
    }

    private Bson generateUpdateFingerPrint(String infoType, Map<String, Object> tuple)
    {
        Bson updateFingerPrint = null;
        if (PRIMARYDEVICEINFO_FIELD.equals(infoType) || NETWORKINFO_FIELD.equals(infoType)
                || DEVICECONFIGRECORD_FIELD.equals(infoType))
        {
            String fingerPrint = (String) tuple.get(FINGERPRINT_FIELD);

            updateFingerPrint = set(infoType, fingerPrint);
        }
        else if (SECONDARYDEVICEINFO_FIELD.equals(infoType))
        {
            String attachedDevicesJsonStr = tuple.get(ATTACHEDDEVICE_FIELD).toString();
            updateFingerPrint = generateUpdateSecondaryDevicesFingerPrint(infoType, attachedDevicesJsonStr);
        }
        return updateFingerPrint;
    }

    /*
     * attachedDeviceJsonStr should look like
     * "attachedDevice": {
        "connectionType": "USB",
        "serialNumber": "2014F27B7F9F",
        "macAddress": "00:04:F2:7A:7F:9F",
        "peripheralType": "VVX Camera",
        "displayName": "Camera",
        "wifiAddress": "00:04:F2:6B:7F:9F",
        "bluetoothAddress": "000666422152",
        "powerSource": "PoE",
        "deviceSignature": "Dev",
        "attachmentState": 0,
        "fingerprint": "283ee18a0684f19f7ededb9229e4a5af"
    }
     * */
    private Bson generateUpdateSecondaryDevicesFingerPrint(String infoType, String attachedDeviceJsonStr)
    {
        ObjectMapper objMapper = JsonUtil.getObjectMapper();
        JsonNode attachedDeviceJsonNode;
        try
        {
            attachedDeviceJsonNode = objMapper.readValue(attachedDeviceJsonStr, JsonNode.class);
        }
        catch (IOException e)
        {
            // TODO implement catch Exception
            log.error("exception when readValue: {} to JsonNode", attachedDeviceJsonStr, e);
            return null;

        }
        JsonNode snJsonNode = attachedDeviceJsonNode.get(SERIALNUMBER_FIELD);
        if (snJsonNode == null)
        {
            log.error("field: {} missing", SERIALNUMBER_FIELD);
            return null;
        }
        JsonNode fingerprintJsonNode = attachedDeviceJsonNode.get(FINGERPRINT_FIELD);
        if (fingerprintJsonNode == null)
        {
            log.info("field: {} missing", FINGERPRINT_FIELD);
            return null;
        }

        String attachedDeviceSN = snJsonNode.textValue();

        String fingerprint = fingerprintJsonNode.textValue();
        return set(infoType + "." + attachedDeviceSN, fingerprint);
        /* JsonNodeFactory factory = JsonUtil.getJsonNodeFactory();
        ArrayNode secdondaryDeviceFingerprints = factory.arrayNode();*/
        /*JsonNode secDeviceJsonNode;
        List<Document> secdondaryDeviceFingerprints = Lists
                .newArrayListWithCapacity(attachedDeviceJsonNode.size());
        for (int i = 0; i < attachedDeviceJsonNode.size(); i++)
        {
            secDeviceJsonNode = attachedDeviceJsonNode.get(i);
             secdondaryDeviceFingerprints
                    .add(factory.objectNode().put(secDeviceJsonNode.get(SERIALNUMBER_FIELD).textValue(),
                            secDeviceJsonNode.get(FINGERPRINT_FIELD).textValue()));
            secdondaryDeviceFingerprints.add(new Document(secDeviceJsonNode.get(SERIALNUMBER_FIELD).textValue(),
                    secDeviceJsonNode.get(FINGERPRINT_FIELD).textValue()));
        }
        return set(infoType, secdondaryDeviceFingerprints);*/
        /*  try
        {
            String secdondaryDeviceFingerprintStr = objMapper.writeValueAsString(secdondaryDeviceFingerprints);
            return set(infoType, secdondaryDeviceFingerprintStr);
        }
        catch (JsonProcessingException e)
        {
            // TODO implement catch JsonProcessingException
            log.error("exception when writeValueAsString: {}", secdondaryDeviceFingerprints, e);
            return null;
        
        }*/

    }

    public static void main(String[] args)
    {

        ObjectMapper objectMapper = new ObjectMapper();
        String s = "[{\"connectionType\":\"USB\",\"serialNumber\":\"2014F27B7F9F\",\"macAddress\":\"00:04:F2:7A:7F:9F\",\"peripheralType\":\"VVX Camera\",\"displayName\":\"Camera\",\"wifiAddress\":\"00:04:F2:6B:7F:9F\",\"bluetoothAddress\":\"000666422152\",\"powerSource\":\"PoE\",\"deviceSignature\":\"Dev\",\"attachmentState\":0,\"fingerprint\":\"283ee18a0684f19f7ededb9229e4a5af\"}]";
        JsonNode node;
        try
        {
            node = objectMapper.readValue(s, JsonNode.class);
        }
        catch (JsonParseException e)
        {
            // TODO implement catch JsonParseException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        catch (JsonMappingException e)
        {
            // TODO implement catch JsonMappingException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        catch (IOException e)
        {
            // TODO implement catch IOException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        JsonNode n = node.get(0);
        System.out.println(node.size());
        System.out.println(n.get("serialNumber").textValue());
        System.out.println(n.get(FINGERPRINT_FIELD));
        ArrayNode secs = JsonUtil.getJsonNodeFactory().arrayNode();
        secs.add(JsonUtil.getJsonNodeFactory().objectNode().put(n.get("serialNumber").textValue(),
                n.get(FINGERPRINT_FIELD).textValue()));
        String str;
        try
        {
            str = JsonUtil.getObjectMapper().writeValueAsString(secs);
        }
        catch (JsonProcessingException e)
        {
            // TODO implement catch JsonProcessingException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        System.out.println(str);

        String d = "[{\"64167f093255\":\"283ee18a0684f19f7ededb9229e4a5af\"}, {\"64167f093266\":\"283ee18a0684f19f7ededb9229e12345\"}]";
        JavaType listKeyValType = JsonUtil.getObjectMapper().getTypeFactory().constructCollectionType(List.class,
                KeyValWrapper.class);
        try
        {
            List<KeyValWrapper> list = JsonUtil.getObjectMapper().readValue(d, listKeyValType);
            System.out.println(list);
        }
        catch (JsonParseException e)
        {
            // TODO implement catch JsonParseException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        catch (JsonMappingException e)
        {
            // TODO implement catch JsonMappingException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
        catch (IOException e)
        {
            // TODO implement catch IOException
            log.error("Unexpected Exception", e);
            throw new UnsupportedOperationException("Unexpected Exception", e);

        }
    }

    @Override
    protected WriteModel<Document> convertWriteModel(Map<String, Object> tuple)
    {
        if (!isRequiredFieldsReady(tuple))
        {
            return null;
        }
        if (!isValidInfoType(tuple))
        {
            return null;
        }
        return generateWriteModel(tuple);

    }

    private boolean isRequiredFieldsReady(Map<String, Object> tuple)
    {
        for (String fieldName : REQUIRE_FIELD_SET)
        {
            if (StringUtils.isEmpty(((String) tuple.get(fieldName))))
            {
                log.error("field: {} has NULL or empty value", fieldName);
                return false;
            }
        }
        return true;
    }

    private boolean isValidInfoType(Map<String, Object> tuple)
    {
        if (VALID_INFOTYPE_VALUE_SET.contains(tuple.get(INFOTYPE_FIELD)))
        {
            return true;
        }
        return false;
    }

}
