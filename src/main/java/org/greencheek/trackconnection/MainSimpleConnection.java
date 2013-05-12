package org.greencheek.trackconnection;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.HashMap;

import java.util.Map;

/**
 * User: dominictootell
 * Date: 12/05/2013
 * Time: 08:34
 *
 * run with:
 *
 * delimiter $$
 *
 *   CREATE DATABASE `testingconn` CHARACTER SET utf8;
 *   use testingconn;
 *   CREATE TABLE `items` ( `item` varchar(128) DEFAULT NULL) ENGINE=InnoDB;
 *   insert into items values ('blah');
 *
 **/
public class MainSimpleConnection {


    public static void main(String[] args) {
        countMany(100);
        createGC();
        getConnectionTrackingMapSize();
        countMany(100);
        createGC();
        getConnectionTrackingMapSize();
        countMany(100);
        createGC();
        getConnectionTrackingMapSize();
        countMany(100);
        createGC();
        getConnectionTrackingMapSize();
        countMany(2000);
        createGC();
        getConnectionTrackingMapSize();
    }


    static {
        // using mysql driver - download and add in classpath
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    public static void countMany(int loops) {
        for(int i = 0; i<loops;i++) {
            count();
        }
    }

    public static void count() {
        Connection connection=null;
        Statement statement = null;
        ResultSet resultSet=null;
        try {

            //sudo ngrep -d lo0 -q -W byline port 3306//
            String url = System.getProperty("jdbc.url","jdbc:mysql://localhost:3306/testingconn?useCompression=true");
            connection = DriverManager.getConnection(url);

            statement = connection.createStatement();
            resultSet = statement.executeQuery("select /* a very large string so that we can see that compression is still enabled on the driver and that all things are equal with the WeakReference added to the CompressedInputStream */ count(*) from items");

            while (resultSet.next()) {
                int rows = resultSet.getInt(1);
                System.out.println(rows);
            }

        } catch(Exception sqe) {
            sqe.printStackTrace();
        }
        finally {
            try {
                if (resultSet!=null) resultSet.close();
            }catch(Exception e){}

            try {
                if(statement!=null) statement.close();
            }catch(Exception e){}

            try{
                if(connection!=null) connection.close();
            }catch(Exception e){}
        }
    }

    public static void createGC() {
        // Weak and a Phantom to show that they are cleared when the GC occurred.
        WeakReference<String> referenceTest = new WeakReference<String>(new String("hey there"));
        ReferenceQueue<String> queue = new ReferenceQueue<String>();
        PhantomReference<String> phantomreferenceTest = new PhantomReference<String>(new String("hey there phantom"),queue);



        // Map that let us create and force a GC to occur.
        WeakReference<Map<Object,String>> myMap = new WeakReference<Map<Object, String>>(new HashMap<Object, String>(10000));
        int i = 0;
        while (true) {
            Map<Object,String> map = myMap.get();
            if (map!=null) {
                map.put(new Object(), "test" + i);
                map=null;
                // try get 10mb
                byte[] b = new byte[1024 * 1024 * 10];
            } else {
                System.out.println("*******GC OCCURRED*******");
                break;
            }
        }

        Reference<? extends String> referenceFromQueue = null;
//        System.out.println("Local Phantom Enqueued: " + phantomreferenceTest.isEnqueued());
        while ( (referenceFromQueue = queue.poll()) != null) {
            System.out.println("Local Phantom cleared:" + referenceFromQueue.get());
            referenceFromQueue.clear();

        }

        if(referenceTest.get() == null) {
            System.out.println("Local Weak Reference cleared:" + referenceTest.get());
        }
    }


    public static void getConnectionTrackingMapSize() {
        try {
            Field f = com.mysql.jdbc.NonRegisteringDriver.class.getDeclaredField("connectionPhantomRefs"); //NoSuchFieldException
            f.setAccessible(true);
            Map connectionTrackingMap = (Map) f.get(com.mysql.jdbc.NonRegisteringDriver.class); //IllegalAccessException

            System.out.println("MAP: " + connectionTrackingMap.size());
        } catch (Exception e) {

        }


    }
}

