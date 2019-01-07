package javaS3;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class S3FileStream 
{
	final static AmazonS3 	s3 = Utils.getS3Client();
	
	private static final Logger log = Logger.getLogger( S3FileStream.class );
	
	//private S3Object				s3object;
	private String					path;
	private boolean					exists;
	private boolean					isDir;
	private long					length;
	private String 					bucket;
	
	private static S3StreamPool					streamPool = new S3StreamPool();
	
	
	private static Map<String, S3FileStream> 	files = new HashMap<>();
	
	private static String getKey( String path )	{ return path.replaceFirst( "^/", "" ); }
	
	public boolean exists( ) { return exists; }
	
	public boolean isDir( )	{ return isDir; }
	
	public long getLength( ) { return length; }
	
	public String getBucket( ) { return bucket; }
	
	//public S3Object getS3object() { return s3object; }
	
	public static S3FileStream getFile( String bucket, String path )
	{
		S3FileStream file = files.get( bucket+path );
		if( file == null )
		{
			file = new S3FileStream();
			file.path = path;
			file.bucket = bucket;
			
			String key = getKey( file.path );
			
			if( key.isEmpty() || !key.contains(".") )
			{
				file.isDir = true;
			} else {
				//GetObjectRequest request = new GetObjectRequest( Utils.getObjectId(bucket, key) );
				S3Object s3object = s3.getObject( bucket, key );
				ObjectMetadata meta = s3object.getObjectMetadata();
				try {
					s3object.close();
				} catch (IOException e) {
					log.error( "failed to close s3object", e );
				}
				
				if( meta != null )
				{
			        file.length = meta.getContentLength();
			        file.isDir = false;
			        file.exists = true;
				} else {
					file.exists = false;
					file.isDir = true;
				}
			}
			files.put( bucket+path, file );
		}
		
		return file;
	}
	
	public byte[] read( long offset, long length )
	{
		byte[] buffer = null;
		
		if( offset >= this.length )
			return null;
		
		length = Math.min( this.length - offset, length );
		
		S3Stream stream = null;
		try {
			stream = streamPool.get(bucket, getKey(path), offset, length);
			
			if( stream == null )
			{
				log.error( "failed to open stream for: " + path );
				return null;
			}
			
			buffer = stream.read(offset, length);
			if( buffer == null )
				return null;
			
			if( buffer.length < length )
			{
				log.info( "bytesRead neq to length: " + buffer.length + " - " + length );
				buffer = Arrays.copyOf( buffer, buffer.length );
			}
			
		} catch (Exception e) {
			log.error( "failed to read from S3Stream", e );
			buffer = null;
		}
		
		return buffer;
	}

	
}
