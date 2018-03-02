package com.polycom.analytics.core.apex.event.fingerprint;

import static com.polycom.analytics.core.apex.common.Constants.ATTACHEDSERIALNUMBER_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.ATTACHMENTSTATE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.EVENTTYPE_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.FINGERPRINTS_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.SECONDARYDEVICEINFO_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.polycom.analytics.core.apex.command.CommandGenerator;
import com.polycom.analytics.core.apex.util.KeyValWrapper;

@Stateless
public class FingerprintChecker extends BaseOperator
{
    private static final Logger log = LoggerFactory.getLogger(FingerprintChecker.class);

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
        deviceAttachmentFingerprintCheck(tuple);
    }

    private void deviceAttachmentFingerprintCheck(Map<String, Object> tuple)
    {

        Integer attachmentState = (Integer) tuple.get(ATTACHMENTSTATE_FIELD);
        if (attachmentState != null && attachmentState.intValue() != 0)
        {
            KeyValWrapper<String> fingerprintInEvent = extractFingerprintFromTuple(tuple);
            if (fingerprintInEvent != null)
            {
                KeyValWrapper<String> fingerprintInDB = extractFingerprintFromDB(tuple);
                if (fingerprintInEvent.equals(fingerprintInDB))
                {
                    log.info("fingerprintInEvent: {} == fingerprintInDB: {}", fingerprintInEvent, fingerprintInDB);
                    return;
                }
                sendCmd(tuple);
            }
        } // this check has already done in DeviceAttachmentEventRouter.routeEvent(), here just re-check
    }

    private void sendCmd(Map<String, Object> tuple)
    {
        String infoType = SECONDARYDEVICEINFO_FIELD;
        String trigger = (String) tuple.get(EVENTTYPE_FIELD) + "Event";
        String attchedSn = (String) tuple.get(ATTACHEDSERIALNUMBER_FIELD);
        String deviceId = (String) tuple.get(DEVICEID_FIELD);
        String tenantId = (String) tuple.get(TENANTID_FIELD);
        String sendInfoCmd = CommandGenerator.generateSendInfoCmd(infoType, trigger, attchedSn, deviceId,
                tenantId);
        log.info("sendInfo: {}", sendInfoCmd);
        output.emit(sendInfoCmd);
    }

    private KeyValWrapper<String> extractFingerprintFromDB(Map<String, Object> tuple)
    {
        @SuppressWarnings("unchecked")
        /*
         * Although type of SECONDARYDEVICEINFO_FIELD is JSONObject when inserting to tuple, but the actual type is LinkedHashMap here
         * */
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

        log.info("NOT found {} info from DB", SECONDARYDEVICEINFO_FIELD);
        return null;

    }

    private KeyValWrapper<String> extractFingerprintFromTuple(Map<String, Object> tuple)
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
    }

}
