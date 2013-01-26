package sar.security;

import java.security.MessageDigest;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class AESCrypter{

	public static byte[] encrypt(byte[] plainTextBytes, String password) throws Exception {
		MessageDigest md = MessageDigest.getInstance("md5");
		byte[] digestOfPassword = md.digest(password.getBytes("utf-8"));

		SecretKey key = new SecretKeySpec(digestOfPassword, "AES");
		IvParameterSpec iv = new IvParameterSpec(new byte[16]);
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		cipher.init(Cipher.ENCRYPT_MODE, key, iv);

		byte[] cipherText = Base64.encodeBase64(cipher.doFinal(plainTextBytes));
		return cipherText;
	}

	public static byte[] decrypt(byte[] message, String password) throws Exception {
		MessageDigest md = MessageDigest.getInstance("md5");
		byte[] digestOfPassword = md.digest(password.getBytes("utf-8"));

		SecretKey key = new SecretKeySpec(digestOfPassword, "AES");
		IvParameterSpec iv = new IvParameterSpec(new byte[16]);
		Cipher decipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		decipher.init(Cipher.DECRYPT_MODE, key, iv);

		byte[] plainText = decipher.doFinal(Base64.decodeBase64(message));
		return plainText;
	}
}