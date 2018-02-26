package com.polycom.analytic.data.mongo;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.Connectable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

/**
 * @author plcm
 *this class extends capabilities of SSL and auth
 *
 *tested on driver version 3.6.0
 */
public class EnhancedMongoDBConnectable implements Connectable
{

    private static final Logger log = LoggerFactory.getLogger(EnhancedMongoDBConnectable.class);

    protected String hostName;
    protected String dataBase;

    protected String userName;
    protected String passWord;
    protected transient MongoClient mongoClient;

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getPassWord()
    {
        return passWord;
    }

    public void setPassWord(String passWord)
    {
        this.passWord = passWord;
    }

    public String getDataBase()
    {
        return dataBase;
    }

    public void setDataBase(String dataBase)
    {
        this.dataBase = dataBase;
    }

    public String getHostName()
    {
        return hostName;
    }

    public void setHostName(String dbUrl)
    {
        this.hostName = dbUrl;
    }

    public String getAuthDB()
    {
        return authDB;
    }

    public void setAuthDB(String authDB)
    {
        this.authDB = authDB;
    }

    protected String authDB;

    protected transient MongoDatabase mongoDb;

    protected SSLSocketFactory createSSLSocketFactory()
    {
        SSLContext sslContext = null;
        try
        {
            sslContext = SSLContext.getInstance("SSL");
            TrustManager[] trustMgr = new TrustManager[] { new X509TrustManager()
            {

                @Override
                public void checkClientTrusted(X509Certificate[] chain,
                        String authType) throws CertificateException
                {

                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain,
                        String authType) throws CertificateException
                {

                }

                @Override
                public X509Certificate[] getAcceptedIssuers()
                {
                    return null;

                }

            } };
            sslContext.init(null, trustMgr, new java.security.SecureRandom());
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
        return sslContext.getSocketFactory();
    }

    private static class MongoHost
    {
        private String replSetName;
        List<ServerAddress> replHosts = Collections.EMPTY_LIST;

        private MongoHost()
        {
        }

        @Override
        public String toString()
        {
            return "MongoHost [replSetName=" + replSetName + ", replHosts=" + replHosts + "]";
        }

        /*
         * format of hostName could be following:
         * -hanalyticsrs0/192.168.60.24:27017,192.168.60.25:27017,192.168.60.26:27017
         * -192.168.60.24:27017,192.168.60.25:27017,192.168.60.26:27017
         * -192.168.60.24:27017
         * */
        public static MongoHost fromString(String hostName)
        {
            Preconditions.checkArgument(StringUtils.isNotBlank(hostName), "HostName of mongoDB is blank");
            hostName = StringUtils.trim(hostName);
            MongoHost host = new MongoHost();
            int slashIndex = StringUtils.indexOf(hostName, '/');
            if (slashIndex > 0)
            {
                host.replSetName = StringUtils.substring(hostName, 0, slashIndex);

            }
            String[] serverAddrs = StringUtils.split(StringUtils.substring(hostName, slashIndex + 1), ',');
            Preconditions.checkArgument(ArrayUtils.isNotEmpty(serverAddrs), "No mongoDB server address defined");
            List<ServerAddress> serverAddrList = Lists.newArrayListWithCapacity(serverAddrs.length);
            String ipPortStr;
            int sep = -1;

            for (int i = 0; i < serverAddrs.length; i++)
            {
                ipPortStr = serverAddrs[i].trim();
                sep = StringUtils.indexOf(ipPortStr, ':');
                Preconditions.checkArgument(sep > 0 && sep < ipPortStr.length() - 1,
                        "server address should be ip:port");
                serverAddrList.add(i, new ServerAddress(StringUtils.left(ipPortStr, sep),
                        Integer.parseInt(StringUtils.substring(ipPortStr, sep + 1))));
            }
            host.replHosts = serverAddrList;
            log.info("Mongo host setting : {}", host);
            return host;
        }
    }

    @Override
    public void connect()
    {

        MongoHost host = MongoHost.fromString(hostName);
        MongoCredential credential = null;
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(passWord) && StringUtils.isNotBlank(authDB))
        {
            credential = MongoCredential.createCredential(userName, authDB, passWord.toCharArray());
        }
        Builder builder = MongoClientOptions.builder();
        builder.sslInvalidHostNameAllowed(true).socketFactory(createSSLSocketFactory()).sslEnabled(true);

        if (StringUtils.isNotBlank(host.replSetName))
        {
            builder.requiredReplicaSetName(host.replSetName);
            builder.readPreference(ReadPreference.nearest());
        }

        mongoClient = new MongoClient(host.replHosts,
                credential == null ? Collections.EMPTY_LIST : Collections.singletonList(credential),
                builder.build());
        //db = mongoClient.getDB(dataBase);
        mongoDb = mongoClient.getDatabase(dataBase);
    }

    @Override
    public boolean isConnected()
    {

        try
        {
            mongoDb.runCommand(new Document("ping", 1));
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }

    @Override
    public void disconnect()
    {
        mongoClient.close();
    }

}
