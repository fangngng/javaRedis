package com.fangngng.javaredis.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class AOFPersistent {

    private AOFData aof ;

    public AOFData newAOF(){

        AOFData aofData = new AOFData();
        String filePath = "aof.aof";
        // new file
        File file = new File(filePath);
        if(!file.exists()){
            try {
                boolean newFile = file.createNewFile();
                if(!newFile){
                    System.out.println("create file fail");
                    throw new RuntimeException("create file fail");
                }
                System.out.println("create file success");
            } catch (IOException e) {
                System.out.println("create file error");
                throw new RuntimeException("create file error");
            }
        }
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(file, true);
            aofData.setFileOutputStream(fileOutputStream);
            aofData.setByteBuffer(ByteBuffer.allocate(1024));
            aofData.setReadWriteLock(new ReentrantReadWriteLock());
            aofData.setAofFile(file);
        }catch (IOException e){
            throw new RuntimeException("read file error");
        }

        this.aof = aofData;
        return aofData;
    }

    public void close() throws IOException {
        this.aof.getFileOutputStream().flush();
        this.aof.getFileOutputStream().close();
    }

    public void write(byte[] data){
        ReentrantReadWriteLock.WriteLock writeLock = aof.getReadWriteLock().writeLock();
        writeLock.lock();

        try{
            aof.getFileOutputStream().write(data);
            aof.getFileOutputStream().write("\r\n".getBytes(StandardCharsets.UTF_8));
            aof.getFileOutputStream().flush();
        }catch (IOException e){
            throw new RuntimeException("write aof failed", e);
        }finally {
            writeLock.unlock();
        }
    }

    public void read(){
        if(this.aof == null){
            return;
        }
        File aofFile = this.aof.getAofFile();
        try {
            Scanner scanner = new Scanner(aofFile);
            while (scanner.hasNextLine()){
                String str = scanner.nextLine();
                RespValue read = new RESPRead().read(str);

                String command = read.getArray()[0].getBulk().toLowerCase();
                Function<RespValue, RespValue> handler = CommandHandler.handlers.get(command);
                if(handler == null){
                    continue;
                }
                try {
                    handler.apply(read);
                } catch (Exception e) {
                    System.out.println("command 执行错误,"+read.toString());
                    continue;
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        System.out.println("aof 初始化完毕");
    }

    public static class AOFData {
        private FileOutputStream fileOutputStream;

        private ByteBuffer byteBuffer;

        private ReentrantReadWriteLock readWriteLock;

        private File aofFile;

        public FileOutputStream getFileOutputStream() {
            return fileOutputStream;
        }

        public void setFileOutputStream(FileOutputStream fileOutputStream) {
            this.fileOutputStream = fileOutputStream;
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public void setByteBuffer(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
        }

        public ReentrantReadWriteLock getReadWriteLock() {
            return readWriteLock;
        }

        public void setReadWriteLock(ReentrantReadWriteLock readWriteLock) {
            this.readWriteLock = readWriteLock;
        }

        public File getAofFile() {
            return aofFile;
        }

        public void setAofFile(File aofFile) {
            this.aofFile = aofFile;
        }
    }
}
