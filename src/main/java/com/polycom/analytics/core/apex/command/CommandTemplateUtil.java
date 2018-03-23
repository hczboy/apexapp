package com.polycom.analytics.core.apex.command;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Throwables;

public final class CommandTemplateUtil
{
    private static final String CMD_TEMPLATE_FILE = "commandToDevice.properties";
    private static Properties commandTplProp;
    /* private static final Logger log = LoggerFactory.getLogger(CommandTemplateUtil.class);
    
    public static final String INFOTYPE_CMD = "infoType_cmd";
    public static final String SERIALNUMBER_CMD = "serialNumber_cmd";*/

    public enum CommandType
    {
        sendInfo,
        startUploadLog,
        sendCurrentLogs,
        setLogLevel,
        stopUploadLog,
        sendDeviceConfigRecord,
        startCallEvents,
        reboot
    }

    static
    {
        commandTplProp = new Properties();
        InputStream in = CommandTemplateUtil.class.getClassLoader().getResourceAsStream(CMD_TEMPLATE_FILE);
        try
        {
            commandTplProp.load(in);
        }
        catch (IOException e)
        {
            Throwables.propagate(e);
        }
    }

    private CommandTemplateUtil()
    {
    }

    public static String getCmdTpl(CommandType cmdType)
    {
        return commandTplProp.getProperty(cmdType.toString());
    }

    /*  private static String generateCmd(CommandType cmdType, Object... args)
    {
        String cmdTpl = commandTplProp.getProperty(cmdType.toString());
        return String.format(cmdTpl, args);
    }
    */
    /* public static String generateCmd(Map<String, Object> tuple, CommandType cmdType)
    {
    
        if (null != cmdType)
        {
            if (cmdType.equals(CommandType.sendInfo))
            {
                return generateSendInfoCmd(tuple);
            }
            log.warn("commandType: {} is NOT supported", cmdType);
            return null;
        }
        log.error("commandType is null for tuple {}", tuple);
        return null;
    }*/

    /* private static String generateSendInfoCmd(Map<String, Object> tuple)
    {
        String trigger = (String) tuple.get(EVENTTYPE_FIELD) + "Event";
        String infoType = (String) tuple.get(INFOTYPE_CMD);
        String sn = (String) tuple.get(SERIALNUMBER_CMD);
        String deviceId = (String) tuple.get(DEVICEID_FIELD);
        String tenantId = (String) tuple.get(TENANTID_FIELD);
        String sendInfoCmd = generateCmd(CommandType.sendInfo, infoType, trigger, sn, deviceId, tenantId);
        log.info("sendInfo: {}", sendInfoCmd);
        return sendInfoCmd;
    }
    
    public static Map<String, Object> constructCmdTupleFromIncomingTuple(Map<String, Object> incomingTuple,
            CommandType cmdType)
    {
        Map<String, Object> cmdTuple = Maps.newHashMap();
    
        cmdTuple.put(DEVICEID_FIELD, incomingTuple.get(DEVICEID_FIELD));
        cmdTuple.put(TENANTID_FIELD, incomingTuple.get(TENANTID_FIELD));
        if (CommandType.sendInfo.equals(cmdType))
        {
            cmdTuple.put(EVENTTYPE_FIELD, incomingTuple.get(EVENTTYPE_FIELD));
        }
        return cmdTuple;
    
    }*/

}
