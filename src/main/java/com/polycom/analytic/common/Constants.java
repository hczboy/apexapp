package com.polycom.analytic.common;

public interface Constants
{
    //following are field names are defined in Kafka message
    final String SEVERITY_FIELD = "severity";
    final String EVENTTIME_FIELD = "eventTime";
    final String EVENTTYPE_FIELD = "eventType";
    final String DEVICEID_FIELD = "deviceID";
    final String TENANTID_FIELD = "tenantID";
    final String CUSTOMERID_FIELD = "customerID";
    final String SERIALNUMBER_FIELD = "serialNumber";
    final String INFOTYPE_FIELD = "infoType";
    final String FINGERPRINT_FIELD = "fingerprint";
    final String ATTACHEDDEVICE_FIELD = "attachedDevice";
    final String ATTACHEDSERIALNUMBER_FIELD = "attachedSerialNumber";
    final String ATTACHMENTSTATE_FIELD = "attachmentState";
    final String FINGERPRINTS_FIELD = "fingerprints";

    //followings are defined all values of field "eventType" 
    final String EVENTTYPE_DEVICEATTACHMENT = "deviceAttachment";
    final String MESSAGE_FIELD = "message";
}
