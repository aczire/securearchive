package com.aczire.sar.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.*;

public class GZipLib{
	public static byte[] compress(byte[] message) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		GZIPOutputStream gzip = new GZIPOutputStream(out);
		gzip.write(message);
		gzip.close();
		return out.toByteArray();
	}

	public static byte[] decompress(byte[] message) throws IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(message);
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		GZIPInputStream gzip = new GZIPInputStream(in);

		byte[] buffer = new byte[1024];
		int bytesRead;

		while ((bytesRead = gzip.read(buffer)) != -1)
		{
			outStream.write(buffer, 0, bytesRead);
		}

		return outStream.toByteArray();
	}
}