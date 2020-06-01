package flink.tracing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Tool {
    public static void main(String[] args) {
        System.out.println("SHA256(1)="+getText("1"));
        System.out.println("SHA256(2)="+getText("2"));
        System.out.println("SHA256(3)="+getText("!@#$%^&*()_+1234567890abcdefghijklmnopqrstuvwxyz================================================================================================="));
    }

     static String getText(String str){
        MessageDigest messageDigest;

        String encodeTxt="";
        try {
            messageDigest= MessageDigest.getInstance("MD5");
            messageDigest.update(str.getBytes(StandardCharsets.UTF_8));

            encodeTxt=byteToHex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return encodeTxt;
    }

    private static String byteToHex(byte[] bytes){
        StringBuilder builder = new StringBuilder();

        String temp;

        for (byte b:bytes){
            temp=Integer.toHexString(b&0xFF).toUpperCase();

            //得到一位的进行补0操作
            if(temp.length()==1)
                builder.append(0);

            builder.append(temp);
        }

        return builder.toString();
    }
}

