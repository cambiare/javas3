package javaS3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

public class S3FileStream 
{
	
	final static Map<String, List<S3FileCache>> fileSystemCache = new HashMap<>();
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private static final Logger log = Logger.getLogger( S3FileStream.class );
	
	private S3Object				s3object;
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
	
	public S3Object getS3object() { return s3object; }
	
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
				file.s3object = s3.getObject( bucket, key );
				ObjectMetadata meta = file.s3object.getObjectMetadata();
				
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
			return new byte[0];
		
		S3Stream stream = null;
		try {
			stream = streamPool.get(bucket, getKey(path), offset, length);
			
			if( stream == null )
			{
				log.error( "failed to open stream for: " + path );
				return new byte[0];
			}
			
			buffer = new byte[(int)length];
			int b = -1;
			int bytesRead = 0;
			while( bytesRead < length && (b = stream.read(offset+bytesRead)) != -1 )
				buffer[bytesRead++] = (byte)b;
						
			if( bytesRead < length )
			{
				log.info( "bytesRead neq to length: " + bytesRead + " - " + length );
				buffer = Arrays.copyOf( buffer, bytesRead );
			}
			
		} catch (Exception e) {
			log.error( "failed to read from S3Stream", e );
			buffer = null;
		}
		
		if( buffer == null )
			return new byte[0];
		
		return buffer;
	}

	
}
