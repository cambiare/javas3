package javaS3;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class S3FileBuffer 
{
	
	private static final int MAX_READAHEAD_SIZE = 1 * 1024 * 1024; // 1MB
	
	final static Map<String, List<S3FileCache>> fileSystemCache = new HashMap<>();
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private static final Logger log = Logger.getLogger( S3FileBuffer.class );
	
	private S3Object				s3object;
	private String					path;
	private S3ObjectInputStream		stream;
	private boolean					exists;
	private boolean					isDir;
	private long					length;
	private String 					bucket;
	private static Map<String, S3FileBuffer> 	files = new HashMap<>();
	
	private static String getKey( String path )	{ return path.replaceFirst( "^/", "" ); }
	
	public boolean exists( ) { return exists; }
	
	public boolean isDir( )	{ return isDir; }
	
	public long getLength( ) { return length; }
	
	public String getBucket( ) { return bucket; }
	
	public S3Object getS3object() { return s3object; }
	
	public static S3FileBuffer getFile( String bucket, String path )
	{
		S3FileBuffer file = files.get( path );
		if( file == null )
		{
			file = new S3FileBuffer();
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
			files.put( path, file );
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
		long s3ReadOffset = offset;
		
		dumpCacheStats();
		
		String key = getKey(path);
		
		byte[] buffer;
		
		if( offset > this.length )
		{
			log.error( "attempting to read beyond file length: " + offset + " file length: " + this.length );
			return new byte[0];
		}
		
		if( this.length < offset+length )
			length = this.length - offset;
		
		buffer = readFromCache( offset, length );
		if( buffer != null )
		{
			log.info( "cache hit: " + path );
			if( buffer.length == length )
				return buffer;
			
			log.info( "not full cache hit" );
			
			s3ReadOffset = offset + buffer.length;
			
		}
		
		log.info( "cache miss: " + path );
		try {
			GetObjectRequest request = new GetObjectRequest(bucket, key).withRange( s3ReadOffset, MAX_READAHEAD_SIZE );
			S3Object o = s3.getObject( request );
			
			buffer = new byte[MAX_READAHEAD_SIZE];
			BufferedInputStream in = new BufferedInputStream( o.getObjectContent() );
			
			int bytesRead = in.read( buffer, 0, MAX_READAHEAD_SIZE );
			
			log.info( "bytes read: " + bytesRead );
			
			if( bytesRead < MAX_READAHEAD_SIZE )
				buffer = Arrays.copyOf( buffer, bytesRead );
			
			insertFileCache( buffer, offset );
			
		} catch( Exception e ) { 
			log.error( "failed to read: ", e );
		}
		
		return readFromCache( offset, length );
	}
	
	private byte[] readFromCache( Long offset, long length )
	{
		List<S3FileCache> fileCache = fileSystemCache.get( path );
		if( fileCache == null )
			return null;
		
		log.info( "found cache for file: " + path );
		
		int idx = Collections.binarySearch( fileCache, offset );
		if( idx < 0 )
			return null;
		
		log.info( "found index of element with binary search" );
		
		S3FileCache cacheHit = fileCache.get( idx );
		if( !cacheHit.withinCache( offset+length ) )
			length = cacheHit.getOffset() + cacheHit.getCacheData().length - offset;
		
		log.info( "found usable file cache object" );
		
		long cacheArrayOffset = offset - cacheHit.getOffset();
		
		return Arrays.copyOfRange( cacheHit.getCacheData(), (int)cacheArrayOffset, (int)cacheArrayOffset + (int)length );
	}
	
	private void insertFileCache( byte[] data, Long offset )
	{		
		S3FileCache newCacheElement = new S3FileCache();
		newCacheElement.setCacheData( data );
		newCacheElement.setOffset( offset );
		
		List<S3FileCache> cachedByteList = fileSystemCache.get( path );
		if( cachedByteList == null || cachedByteList.size() <= 0 )
		{
			cachedByteList = new LinkedList<>();
			fileSystemCache.put( path, cachedByteList );
			
			cachedByteList.add( newCacheElement );
			return;
		}
		
		// check first to see if this is being appended to avoid searching - may be a likely case
		int cacheListSize = cachedByteList.size();
		if( cacheListSize > 0 && cachedByteList.get( cacheListSize-1 ).compareTo( offset ) > 0 )
		{
			cachedByteList.add( newCacheElement );
			return;
		}
		
		
		int offsetIdx = Collections.binarySearch( cachedByteList, offset );
		if( offsetIdx > 0 )
		{
			S3FileCache leftWithinCache = cachedByteList.get( offsetIdx );
			
			if( leftWithinCache.withinCache( offset + data.length ) )
			{
				// this insert request is basically invalid.  it means there's already an overlapping cached
				// object that should have been used for the original request.
				log.warn( "cache insert overlap attempt: " + offset );
				return;
			}
			
			// chop the left side of the byte buffer and update the offset
			int leftOffset = (int)(leftWithinCache.getOffset() + leftWithinCache.getCacheData().length);
			newCacheElement.setCacheData( 
					Arrays.copyOfRange( 
							newCacheElement.getCacheData(), 
							leftOffset,
							(int)(newCacheElement.getCacheData().length - (leftOffset - offset)) ) );
		}
		
		offsetIdx = Math.abs( offsetIdx );
		if( cachedByteList.size() > Math.abs( offsetIdx ) + 1 )
		{
			S3FileCache rightWithinCache = cachedByteList.get( offsetIdx + 1 );
			if( rightWithinCache.withinCache( offset + data.length ) )
			{			
				if( rightWithinCache.withinCache( offset ) )
				{
					// this insert request is basically invalid.  it means there's already an overlapping cached
					// object that should have been used for the original request.
					log.warn( "cache insert overlap attempt: " + offset );
					return;
				}
				
				// chop the right side of the byte buffer and update the offset
				newCacheElement.setCacheData( 
						Arrays.copyOfRange( 
								newCacheElement.getCacheData(), 
								0,
								(int)( data.length - (rightWithinCache.getOffset() - offset) ) ) );
			}
		}
		
		cachedByteList.add( offsetIdx, newCacheElement );
	}
	
	private void dumpCacheStats( )
	{
		final String tab = "---- ";
		
		log.info( "cache: " );
		
		for( String path : fileSystemCache.keySet() )
		{
			log.info( tab + path );
			
			List<S3FileCache> cacheList = fileSystemCache.get( path );
			for( S3FileCache cacheElement : cacheList )
			{
				log.info( tab + tab + cacheElement.getOffset() + tab + cacheElement.getCacheData().length );
			}
		}
		
	}
}








