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
		List<S3Stream> streams = pool.get( bucket+key );
		if( streams != null )
		{
			log.info( "open streams to file: " + streams.size() + " " + key );
			streams = new ArrayList<>();
			pool.put( bucket+key, streams );
		}
		
		for( S3Stream stream : streams )
		{
			if( !stream.isLocked() && offset.equals( stream.getOffset() ) )
			{
				stream.lock();
				return stream;
			}
		}
		
		S3Stream stream = new S3Stream(bucket, key, offset);
		streams.add( stream );

		return stream;
	}
}