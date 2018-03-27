package com.polycom.analytics.core.apex.command;

import static com.polycom.analytics.core.apex.common.Constants.DEVICEID_FIELD;
import static com.polycom.analytics.core.apex.common.Constants.TENANTID_FIELD;

import java.util.Map;

import javax.validation.constraints.NotNull;

import com.polycom.analytics.core.apex.command.CommandTemplateUtil.CommandType;

public class SendCurrentLogsCommandObj extends AbstractCommandObj
{

    public static enum FileType
    {
        log, pcap, tsid, coreDump
    }

    public static enum Compress
    {
        yes, no
    }

    public static enum CompressionAlgorithm
    {
        gzip, none
    }

    @NotNull
    private FileType fileType;
    @NotNull
    private String fileID;
    @NotNull
    private Compress compress;
    @NotNull
    private CompressionAlgorithm compressionAlgorithm;

    public static SendCurrentLogsCommandObj fromTuple(Map<String, Object> incomingTuple)
    {
        SendCurrentLogsCommandObj sendCurrentLogsCmdObj = new SendCurrentLogsCommandObj();
        sendCurrentLogsCmdObj.setDeviceId((String) incomingTuple.get(DEVICEID_FIELD));
        sendCurrentLogsCmdObj.setTenantId((String) incomingTuple.get(TENANTID_FIELD));
        sendCurrentLogsCmdObj.compress = Compress.yes;
        sendCurrentLogsCmdObj.fileType = FileType.log;
        sendCurrentLogsCmdObj.compressionAlgorithm = CompressionAlgorithm.gzip;
        return sendCurrentLogsCmdObj;

    }

    public void setFileID(String fileID)
    {
        this.fileID = fileID;
    }

    @Override
    protected String getCmdString()
    {
        return String.format(CommandTemplateUtil.getCmdTpl(CommandType.sendCurrentLogs), fileType, fileID,
                compress, compressionAlgorithm, deviceId, tenantId);

    }

}
