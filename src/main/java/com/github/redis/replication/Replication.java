package com.github.redis.replication;

import com.github.redis.replication.rdb6.Parser;
import com.github.redis.replication.rdb6.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 15-12-21.
 */
public class Replication {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private String host;
    private int port;

    private Pipeline pipeline;



    public Replication(String host, int port,Pipeline pipeline) {
        this.host = host;
        this.port = port;
        this.pipeline = pipeline ;
    }

    public void doReplication() throws IOException {

        Socket socket = openNetConnection();

        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream() ;
        Tuple<String,Long> fullSyncResult = sendPSyncFullsync(outputStream , inputStream );

        String runId = fullSyncResult.getA() ;
        long offset = fullSyncResult.getB();

        Tuple<String,Long> rdbSize  = readLine(inputStream);
        logger.info("rdb size :", rdbSize.getA());

        dumpRDB(inputStream);

        while (true){
            long n = syncCmd(outputStream, inputStream , offset ) ;

            offset += n ;

            while (true){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                socket = openNetConnection();
                outputStream = socket.getOutputStream() ;
                inputStream = socket.getInputStream();
                break;
            }

            sendPSyncContinue(outputStream , inputStream , runId , offset) ;
        }
    }

    private void sendPSyncContinue(OutputStream outputStream, InputStream inputStream, String runId, long offset) throws IOException {
        outputStream.write(String.format("replconf ack %d\r\n" , offset + 2 ).getBytes());
        outputStream.flush();

        Tuple<String,Long> lineResult = readLine(inputStream) ;

        String[] strings = lineResult.getA().split(" ");
        if( strings.length != 1 || !strings[0].toLowerCase().startsWith("+continue") ){
            throw new RuntimeException(String.format("invalid psync response = '%s', should be continue", lineResult.getA() )) ;
        }

    }

    private long syncCmd(final OutputStream outputStream, final InputStream inputStream, final long offset) {

        final AtomicLong nRead = new AtomicLong(0) ;

        new Thread(){
            @Override
            public void run() {
                while(true){
                    try {
                        Thread.sleep(5 * 1000 );
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    try {
                        sendPSyncAck( outputStream,offset + nRead.get() );
                    } catch (IOException e) {
                        logger.warn("sendPSyncAck fail." , e );
                        break;
                    }
                }

            }
        }.start();


        while (true){

            long n = 0 ;
            Tuple<String, Long> lineResult ;
            try{
                lineResult = readLine(inputStream);
            }catch (Exception e){
                logger.error("readLine fail.", e);
                break;
            }
            n += lineResult.getB();
            String line = lineResult.getA();

            if(line.startsWith("*")){

                int argsSize = Integer.parseInt(line.substring(1)) ;
                String[] cmd = new String[argsSize];

                for(int i = 0 ; i < argsSize ; i++ ){
                    lineResult = readLine(inputStream);
                    n += lineResult.getB();
                    line = lineResult.getA();
                    if(!line.startsWith("$")){
                        throw new RuntimeException("invalid line :" + line );
                    }

                    int len = Integer.parseInt(line.substring(1)) + 2 ;
                    byte[] bytes = readBytes(inputStream , len ) ;
                    cmd[i] = new String( bytes , 0 ,len - 2 ) ;
                    n += len ;
                }
                //System.out.println(Arrays.toString(cmd));
                sendToPipeline(cmd) ;
                nRead.addAndGet(n);
            }else{
                throw new RuntimeException("invalid line :" + line );
            }
        }

        return nRead.get();
    }

    private void sendToPipeline(String[] cmd) {
        try {
            pipeline.process( cmd ) ;
        }catch (Exception e){
            logger.error("pipeline process fail." , e );
        }

    }

    public String[] slice(String[] src , int start ){
        String[] buf = new String[src.length - start ];
        for(int i =  start ; i < src.length ; i++ ){
            buf[i - start ] = src[i];
        }
        return buf ;
    }


    private void dumpRDB(InputStream inputStream) {
        Parser parser = new Parser(inputStream);
        parser.header();

        while (true) {
            Parser.BinEntry binEntry = parser.nextBinEntry();
            if (binEntry == null) {
                break;
            }
            System.out.println(binEntry);
        }

        parser.footer();
    }

    private void sendPSyncAck(OutputStream outputStream , long offset ) throws IOException {
        outputStream.write(String.format("replconf ack %d\r\n", offset).getBytes());
        outputStream.flush();
    }

    private Tuple<String,Long> sendPSyncFullsync(OutputStream outputStream ,InputStream inputStream ) throws IOException {
        outputStream.write(String.format("psync ? -1\r\n").getBytes());
        outputStream.flush();
        Tuple<String,Long> lineResult = readLine(inputStream) ;

        String[] strings = lineResult.getA().split(" ");
        if( strings.length != 3 || !strings[0].toLowerCase().startsWith("+fullresync") ){
            throw new RuntimeException(String.format("invalid psync response = '%s', should be fullsync", lineResult.getA() )) ;
        }

        return Tuple.of(strings[1] , Long.parseLong(strings[2]) ) ;
    }



    private Socket openNetConnection() throws IOException {
        Socket socket = new Socket(host, port);
        socket.setKeepAlive(true);
        socket.setReuseAddress(true);
        socket.setSoTimeout(1 * 60 * 1000);
        socket.setSendBufferSize(4 * 1024);
        socket.setReceiveBufferSize(4 * 1024);
        return socket;
    }

    public Tuple<String,Long> readLine(InputStream inputStream) {
        long n = 0 ;
        StringBuilder sb = new StringBuilder(1024);
        while (!sb.toString().endsWith("\r\n")) {
            try{
                sb.append((char) inputStream.read());
                n++ ;
            }catch (IOException e){
                throw new RuntimeException(e);
            }
        }
        return Tuple.of( sb.substring(0, sb.length() - 2) , n );
    }

    private byte[] readBytes(InputStream inputStream , int len) {
        byte[] buf = new byte[len];
        readFull(inputStream,buf);
        return buf ;
    }

    private void readFull(InputStream inputStream , byte[] buf) {
        int c = 0;
        while (c != buf.length) {
            int n = 0;
            try {
                n = inputStream.read(buf, c, buf.length - c);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (n == -1) {
                throw new RuntimeException("Read " + buf.length + " bytes , but only " + c + " already read.");
            }
            c += n;
        }
    }


}
