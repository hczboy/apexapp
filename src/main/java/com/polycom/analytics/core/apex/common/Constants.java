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
    final String DEVICECONFIGURATIONRECORD_FIELD = "deviceConfigurationRecord";
    final String SECONDARYDEVICEINFO_FIELD = "secondaryDeviceInfo";
    final String PRIMARYDEVICEINFO_FIELD = "primaryDeviceInfo";
    final String ORGANIZATIONID_FIELD = "organizationID";
    final String CALLISSUE_FIELD = "callIssue";
    final String CONNECTIONSTATUS_FIELD = "connectionStatus";
    final String DESCRIPTION_FIELD = "Description";
    final String REBOOTTYPE_FIELD = "rebootType";
    final String STATUS_FIELD = "status";
    final String CALLID_FIELD = "callID";
    final String INGESTIONTIME_FIELD = "ingestionTime";
    final String DEVICECONFIGRECORD_FIELD = "deviceConfigRecord";

    //following are internal use fields
    final String DRUIDDS_INTER_FIELD = "druid_ds";

    //following are all values for DRUIDDS_INTER_FIELD
    //Please be NOTED that, the values here should be in consistent with value of "dataSource" in file server.json
    final String DRUIDDS_OUTOFBOUNDCALLQUALITY = "outOfBoundCallQuality";
    final String DRUIDDS_CALLQUALITY = "callQuality";

    //following fields belong to RTCP-XR fields
    final String LOSSRATE_RTCPXR_FIELD = "lossRate";
    final String DISCARDRATE_RTCPXR_FIELD = "discardRate";
    final String RFACTOR_RTCPXR_FIELD = "rFactor";

    //followings define all values of field "eventType" 
    final String EVENTTYPE_DEVICEATTACHMENT = "deviceAttachment";
    final String EVENTTYPE_REBOOT = "reboot";
    final String EVENTTYPE_SERVICEREGISTRATIONSTATUS = "serviceRegistrationStatus";
    final String EVENTTYPE_DEVICECONFIGRECORD = "deviceConfigRecord";
    final String EVENTTYPE_DEVICEERROR = "deviceError";
    final String EVENTTYPE_INCALLERROR = "inCallError";
    final String EVENTTYPE_CALLCONNECTION = "callConnection";
    final String EVENTTYPE_CALLQUALITY = "callQuality";

    //following define value of field "infoType", which is one of field of Message to Device
    final String INTOTYPE_DEVICEHEALTHINFO = "deviceHealthInfo";

    //followings define all values of field "connectionStatus" 
    final String CONNECTIONSTATUS_CALLSTARTED = "callStarted";
    final String CONNECTIONSTATUS_CALLENDED = "callEnded";

    //
    final String MESSAGE_FIELD = "message";
}
