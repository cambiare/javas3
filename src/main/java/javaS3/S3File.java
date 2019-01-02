package javaS3;

import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3File 
{
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private static final Logger log = Logger.getLogger( S3File.class );
	
	private S3Object				s3object;
	private String					path;
	private S3ObjectInputStream		stream;
	private boolean					exists;
	private boolean					isDir;
	private long					length;
	private String 					bucket;
	
	
	private static String getKey( String path )	{ return path.replaceFirst( "^/", "" ); }
	
	public boolean exists( ) { return exists; }
	
	public boolean isDir( )	{ return isDir; }
	
	public long getLength( ) { return length; }
	
	public String getBucket( ) { return bucket; }
	
	public S3Object getS3object() { return s3object; }
	
	public static S3File getFile( String bucket, String path )
	{
		S3File file = new S3File();
		file.path = path;
		file.bucket = bucket;
		
		String key = getKey( file.path );
		
		if( key.isEmpty() || !key.contains(".") )
		{
			file.isDir = true;
		} else {
			log.info( "testing for key existence: " + key );
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
		
		return file;
	}
	
//	public static String[] listDir( String bucket, String path )
//	{
//		String delimiter = "/";
//		if( !path.endsWith(delimiter) )
//		{
//			path = path + delimiter;
//		}
//		
//		String key = getKey(path);
//		log.info( "readdir called: " + path );
//		
//		ListObjectsRequest lorequest = new ListObjectsRequest().withBucketName(bucket).withPrefix( key ).withDelimiter(delimiter);
//
//		ObjectListing result = s3.listObjects( lorequest );
//		
//		for( String subkey : result.getCommonPrefixes() ) 
//		{
//			subkey = subkey.replaceAll("/$", "");
//			String s3path = subkey.replaceAll("^.*" + delimiter, "");
//			filter.apply( buf, s3path, null, 0 );
//		}
//		
//		for( S3ObjectSummary summary: result.getObjectSummaries() )
//		{
//			String s3path = summary.getKey().replaceAll("^.*/", "" );
//			filter.apply( buf, s3path, null, 0 );
//		}
//	}
	
	public synchronized byte[] read( long offset, long length )
	{
		byte[] buffer = new byte[(int)length];
		
		try {
			if( stream == null )
			{
				stream = s3object.getObjectContent();
			}
			
			int bytesRead = 0;
			bytesRead = stream.read( buffer, (int)offset, (int)length );
			log.info( "bytes read: " + bytesRead );
			
			if( bytesRead < length )
				buffer = Arrays.copyOf( buffer, bytesRead );
		} catch( Exception e ) { 
			log.error( e );
		}
		return buffer;
	}
	
}
