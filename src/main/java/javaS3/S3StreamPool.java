package javaS3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class S3StreamPool 
{
	private Map<String, List<S3Stream>>		pool = new HashMap<>();
	
	private static final Logger log = Logger.getLogger( S3StreamPool.class );

	
	public S3Stream get( String bucket, String key, Long offset )
	{
		try {
			List<S3Stream> streams = pool.get( bucket+key );
			if( streams == null )
			{
				streams = new ArrayList<>();
				pool.put( bucket+key, streams );
			}
			
			log.info( "searching for stream to file: " + streams.size() + " " + key );
			
			for( S3Stream stream : streams )
			{
				if( !stream.isLocked() && offset.equals( stream.getOffset() ) )
				{	
					log.info( "found stream with offsets: " + offset );
					return stream;
				} else {
					log.info( "missed stream with offsets: " + offset );
				}
			}
			
			log.info( "opening new stream to file: " + key );
			S3Stream stream = new S3Stream(bucket, key, offset);
			streams.add( stream );
	
			
			return stream;
		} catch( Exception e ) {
			log.error( "failed to create S3Stream within S3StreamPool: ", e );
		}
		
		return null;
	}
}
