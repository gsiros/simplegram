package com.simplegram.src.MD5;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

//implementation of md5 hashing algorithm
public class MD5 {
    public static String hash(String input)
    {
        try {

            MessageDigest md = MessageDigest.getInstance("MD5");

            //calculate message digest.
            //returns byte array
            byte[] messageDigest = md.digest(input.getBytes());

            //convert byte array into BigInt
            BigInteger num = new BigInteger(1, messageDigest);

            //convert message digest into hex value
            String hashtext = num.toString(16);
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        }
        //wrong message digest algorithm provided
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
