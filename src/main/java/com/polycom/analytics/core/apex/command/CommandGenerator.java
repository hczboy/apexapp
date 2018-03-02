package com.polycom.analytics.core.apex.command;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.google.common.base.Throwables;

public final class CommandGenerator
{
    private static final String CMD_TEMPLATE_FILE = "commandToDevice.properties";
    private static Properties commandTplProp;

    private enum CommandType
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
        InputStream in = CommandGenerator.class.getClassLoader().getResourceAsStream(CMD_TEMPLATE_FILE);
        try
        {
            commandTplProp.load(in);
        }
        catch (IOException e)
        {
            Throwables.propagate(e);
        }
    }

    private CommandGenerator()
    {
    }

    private static String generateCmd(CommandType cmdType, Object... args)
    {
        String cmdTpl = commandTplProp.getProperty(cmdType.toString());
        return String.format(cmdTpl, args);
    }

    public static String generateSendInfoCmd(Object... args)
    {
        return generateCmd(CommandType.sendInfo, args);
    }

}
