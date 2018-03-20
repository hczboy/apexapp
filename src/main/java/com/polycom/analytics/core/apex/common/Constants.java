package com.polycom.analytics.core.apex.common;

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
    final String NETWORKINFO_FIELD = "networkInfo";
    final String SECONDARYDEVICEINFO_FIELD = "secondaryDeviceInfo";
    final String PRIMARYDEVICEINFO_FIELD = "primaryDeviceInfo";
    final String DEVICECONFIGRECORD_FIELD = "deviceConfigRecord";
    final String REBOOTTYPE_FIELD = "rebootType";
    final String STATUS_FIELD = "status";

    //followings define all values of field "eventType" 
    final String EVENTTYPE_DEVICEATTACHMENT = "deviceAttachment";
    final String EVENTTYPE_REBOOT = "reboot";
    final String EVENTTYPE_SERVICEREGISTRATIONSTATUS = "serviceRegistrationStatus";
    final String EVENTTYPE_DEVICECONFIGRECORD = "deviceConfigRecord";
    final String EVENTTYPE_DEVICEERROR = "deviceError";

    //following define value of field "infoType", which is one of field of Message to Device
    final String INTOTYPE_DEVICEHEALTHINFO = "deviceHealthInfo";
    //
    final String MESSAGE_FIELD = "message";
}
