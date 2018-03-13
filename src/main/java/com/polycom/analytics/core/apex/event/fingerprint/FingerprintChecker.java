package com.polycom.analytics.core.apex.event.fingerprint;

import static com.polycom.analytics.core.apex.common.Constants.ATTACHEDSERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.ATTACHMENTSTATE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_DEVICEATTACHMENT;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_REBOOT;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_SERVICEREGISTRATIONSTATUS;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINTS_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.NETWORKINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.PRIMARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SECONDARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;
import com.polycom.analytics.core.apex.command.CommandGenerator;
import com.polycom.analytics.core.apex.util.KeyValWrapper;

@Stateless
public class FingerprintChecker extends BaseOperator
{
    private static final Logger log = LoggerFactory.getLogger(FingerprintChecker.class);

    private static enum FingerprintType
    {
        /*
         * WARN: do NOT change the enum value, the enum value MUST be same with 
         * PRIMARYDEVICEINFO_FIELD, SECONDARYDEVICEINFO_FIELD, NETWORKINFO_FIELD
         * defined in com.polycom.analytics.core.apex.common.Constants
         * 
         * */
        primaryDeviceInfo, secondaryDeviceInfo, networkInfo, deviceConfigRecord;
    }

    public final transient DefaultInputPort<Map<String, Object>> input = new DefaultInputPort<Map<String, Object>>()
    {
        @Override
        public void process(Map<String, Object> tuple)
        {
            processTuple(tuple);
        }
    };

    public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

    protected void processTuple(Map<String, Object> tuple)
    {
        String eventType = (String) tuple.get(EVENTTYPE_FIELD);
        if (EVENTTYPE_DEVICEATTACHMENT.equals(eventType))
        {
            deviceAttachmentFingerprintCheck(tuple);

        }
        else if (EVENTTYPE_REBOOT.equals(eventType) || EVENTTYPE_SERVICEREGISTRATIONSTATUS.equals(eventType))
        {
            generalFingerprintCheck(tuple, FingerprintType.valueOf(PRIMARYDEVICEINFO_FIELD));
            generalFingerprintCheck(tuple, FingerprintType.valueOf(SECONDARYDEVICEINFO_FIELD));
            generalFingerprintCheck(tuple, FingerprintType.valueOf(NETWORKINFO_FIELD));
        }
    }

    private void generalFingerprintCheck(Map<String, Object> tuple, FingerprintType fingerprintType)
    {

        List<KeyValWrapper<String>> fingerprintFromTuple = extractFingerprintFromTuple(tuple, fingerprintType);
        if (CollectionUtils.isNotEmpty(fingerprintFromTuple))
        {
            List<KeyValWrapper<String>> fingerprintFromDB = extractFingerprintFromDB(tuple, fingerprintType);
            Collection<KeyValWrapper<String>> diffColl = CollectionUtils.subtract(fingerprintFromTuple,
                    fingerprintFromDB);
            if (CollectionUtils.isNotEmpty(diffColl))
            {

                for (KeyValWrapper<String> kv : diffColl)
                {
                    sendInfoCmd(tuple, fingerprintType.toString(), kv.getKey());
                }
            }
        }
    }

    /* public static void main(String[] args)
    {
        List<KeyValWrapper<String>> a = Arrays.asList(new KeyValWrapper<>("A", "aa"));
        List<KeyValWrapper<String>> b = Arrays.asList(new KeyValWrapper<>("A", "a1"),
                new KeyValWrapper<>("B", "bb"));
        System.out.println(a.equals(b));
        System.out.println(CollectionUtils.subtract(a, a).size());
    
    }
    */
    private void deviceAttachmentFingerprintCheck(Map<String, Object> tuple)
    {

        Integer attachmentState = (Integer) tuple.get(ATTACHMENTSTATE_FIELD);
        if (attachmentState != null && attachmentState.intValue() != 0)
        {
            String attchedSn = (String) tuple.get(ATTACHEDSERIALNUMBER_FIELD);
            if (StringUtils.isNotEmpty(attchedSn))
            {
                List<KeyValWrapper<String>> secondaryFingerprintsInTuple = extractFingerprintFromTuple(tuple,
                        FingerprintType.secondaryDeviceInfo);

                KeyValWrapper<String> fingerprintInEvent = extractSecondaryFingerprintByAttachedSn(attchedSn,
                        secondaryFingerprintsInTuple);
                if (fingerprintInEvent != null)
                {
                    List<KeyValWrapper<String>> secondaryFingerprintsInDB = extractFingerprintFromDB(tuple,
                            FingerprintType.secondaryDeviceInfo);
                    KeyValWrapper<String> fingerprintInDB = extractSecondaryFingerprintByAttachedSn(attchedSn,
                            secondaryFingerprintsInDB);
                    if (fingerprintInEvent.equals(fingerprintInDB))
                    {
                        log.info("fingerprintInEvent: {} == fingerprintInDB: {}", fingerprintInEvent,
                                fingerprintInDB);
                        return;
                    }
                    sendInfoCmd(tuple, SECONDARYDEVICEINFO_FIELD, attchedSn);
                }
                return;
            }
            log.error("field: {} is null or empty, but expect non-empty string", ATTACHEDSERIALNUMBER_FIELD);
            return;

        } // this check has already done in DeviceAttachmentEventRouter.routeEvent(), here just re-check
    }

    private void sendInfoCmd(Map<String, Object> tuple, String infoType, String sn)
    {

        String trigger = (String) tuple.get(EVENTTYPE_FIELD) + "Event";

        String deviceId = (String) tuple.get(DEVICEID_FIELD);
        String tenantId = (String) tuple.get(TENANTID_FIELD);
        String sendInfoCmd = CommandGenerator.generateSendInfoCmd(infoType, trigger, sn, deviceId, tenantId);
        log.info("sendInfo: {}", sendInfoCmd);
        output.emit(sendInfoCmd);
    }

    private List<KeyValWrapper<String>> extractFingerprintFromDB(Map<String, Object> tuple,
            FingerprintType fingerprintType)
    {
        List<KeyValWrapper<String>> result = Collections.EMPTY_LIST;
        if (FingerprintType.secondaryDeviceInfo.equals(fingerprintType))
        {
            @SuppressWarnings("unchecked")
            Map<String, Object> secondaryFingerprintsInDB = (Map<String, Object>) tuple
                    .get(SECONDARYDEVICEINFO_FIELD);
            if (secondaryFingerprintsInDB != null)
            {
                result = Lists.newArrayListWithCapacity(secondaryFingerprintsInDB.size());
                KeyValWrapper<String> fpKeyVal;
                for (String k : secondaryFingerprintsInDB.keySet())
                {
                    fpKeyVal = new KeyValWrapper<>(k, (String) secondaryFingerprintsInDB.get(k));
                    result.add(fpKeyVal);
                }
                return result;
            }

            log.info("NOT found {} fingerprint from DB", SECONDARYDEVICEINFO_FIELD);
            return result;
        }
        String sn = (String) tuple.get(SERIALNUMBER_FIELD);
        if (StringUtils.isNotEmpty(sn))
        {
            return Arrays.asList(new KeyValWrapper<>(sn, (String) tuple.get(fingerprintType.toString())));
        }
        log.error("field: {} is null or empty, but expected non-empty string", SERIALNUMBER_FIELD);
        return result;
    }

    private KeyValWrapper<String> extractSecondaryFingerprintByAttachedSn(@NotNull String attachedSn,
            List<KeyValWrapper<String>> secFingerprints)
    {

        if (CollectionUtils.isNotEmpty(secFingerprints))
        {

            for (KeyValWrapper<String> secFp : secFingerprints)
            {
                if (attachedSn.equals(secFp.getKey()))
                {
                    return secFp;
                }
            }
            return null;

        }
        return null;
    }

    /*private KeyValWrapper<String> extractFingerprintFromDB(Map<String, Object> tuple)
    {
        @SuppressWarnings("unchecked")
        
         * type of SECONDARYDEVICEINFO_FIELD is fixed when inserting to tuple, the actual type is LinkedHashMap here instead of JSONObject
         * 
        Map<String, Object> secondaryFingerprintsInDB = (Map<String, Object>) tuple.get(SECONDARYDEVICEINFO_FIELD);
        if (secondaryFingerprintsInDB != null)
        {
            String attchedSn = (String) tuple.get(ATTACHEDSERIALNUMBER_FIELD);
            String fingerprintInDB = (String) secondaryFingerprintsInDB.get(attchedSn);
            if (fingerprintInDB != null)
            {
                return new KeyValWrapper<>(attchedSn, fingerprintInDB);
            }
    
            log.info("NOT found fingerprint for attchedSn: {} from DB", attchedSn);
            return null;
    
        }
    
        log.info("NOT found {} fingerprint from DB", SECONDARYDEVICEINFO_FIELD);
        return null;
    
    }*/

    private List<KeyValWrapper<String>> extractFingerprintFromTuple(Map<String, Object> tuple,
            FingerprintType fingerprintType)
    {
        List<KeyValWrapper<String>> result = Collections.EMPTY_LIST;
        JSONObject fingerprintsJsonObj = (JSONObject) tuple.get(FINGERPRINTS_FIELD);
        if (fingerprintsJsonObj != null)
        {
            if (FingerprintType.secondaryDeviceInfo.equals(fingerprintType))
            {
                JSONObject secondaryFingerprintsJsonObj = fingerprintsJsonObj
                        .getJSONObject(SECONDARYDEVICEINFO_FIELD);
                if (secondaryFingerprintsJsonObj != null)
                {
                    result = Lists.newArrayListWithCapacity(secondaryFingerprintsJsonObj.size());
                    KeyValWrapper<String> fpKeyVal;
                    for (String k : secondaryFingerprintsJsonObj.keySet())
                    {
                        fpKeyVal = new KeyValWrapper<>(k, (String) secondaryFingerprintsJsonObj.get(k));
                        result.add(fpKeyVal);
                    }
                    return result;

                }
                log.info("NOT found {} fingerprint from tuple in header {}", SECONDARYDEVICEINFO_FIELD,
                        FINGERPRINTS_FIELD);
                return result;
            }
            String sn = (String) tuple.get(SERIALNUMBER_FIELD);
            if (StringUtils.isNotEmpty(sn))
            {
                return Arrays.asList(
                        new KeyValWrapper<>(sn, (String) fingerprintsJsonObj.get(fingerprintType.toString())));
            }
            log.error("field: {} is null or empty, but expected non-empty string", SERIALNUMBER_FIELD);
            return result;

        }
        log.error("field: {} NOT found, but expected", FINGERPRINTS_FIELD);
        return result;
    }

    /*private KeyValWrapper<String> extractFingerprintFromTuple(Map<String, Object> tuple)
    {
        JSONObject fingerprintsJsonObj = (JSONObject) tuple.get(FINGERPRINTS_FIELD);
        if (fingerprintsJsonObj != null)
        {
            JSONObject secondaryFingerprintsJsonObj = fingerprintsJsonObj.getJSONObject(SECONDARYDEVICEINFO_FIELD);
            if (secondaryFingerprintsJsonObj != null)
            {
                String attchedSn = (String) tuple.get(ATTACHEDSERIALNUMBER_FIELD);
                if (StringUtils.isNotEmpty(attchedSn))
                {
                    String fingerprint = secondaryFingerprintsJsonObj.getString(attchedSn);
                    if (StringUtils.isNotEmpty(fingerprint))
                    {
                        return new KeyValWrapper<>(attchedSn, fingerprint);
                    }
                    log.error("fingerprint for attchedSn: {} NOT found", attchedSn);
                    return null;
                }
                log.error("field: {} NOT found, but expected", ATTACHEDSERIALNUMBER_FIELD);
                return null;
            }
            log.error("No {} fingerprint showing in header {}, but expected", SECONDARYDEVICEINFO_FIELD,
                    FINGERPRINTS_FIELD);
            return null;
    
        }
        log.error("field: {} NOT found, but expected", FINGERPRINTS_FIELD);
        return null;
    }*/

}
