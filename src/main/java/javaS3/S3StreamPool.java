package javaS3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public class S3StreamPool 
{
	private static final int POOL_MEMBER_TIMEOUT = Utils.getProperty( "javas3.pool_member_timeout", 10000 );
	
	private static Map<String, List<S3Stream>>		pool = new HashMap<>();
	
	private static final Logger log = Logger.getLogger( S3StreamPool.class );

	public S3StreamPool( )
	{
		Executors.newSingleThreadExecutor().execute( ()-> { timeoutUnusedStreams(); } );
	}
	
	private void timeoutUnusedStreams( )
	{
		List<S3Stream> deleteList = new ArrayList<>();
		
		while( true )
		{
			try {
				for( String key : pool.keySet() )
				{
					for( S3Stream stream : pool.get(key) )
					{
						if( (System.currentTimeMillis() - stream.getLastReadTime()) > POOL_MEMBER_TIMEOUT )
						{
							stream.close();
							deleteList.add( stream );
						}
					}
					
					for( S3Stream stream : deleteList )
					{
						pool.get(key).remove( stream );
						if( pool.get(key).isEmpty() )
							pool.remove( key );
					}
					
					deleteList.clear();
				}
				
				try {
					Thread.sleep( 1000 );
				} catch (InterruptedException e) {
					log.error( "interrupted in timeoutUnusedStreams", e );
				}
			} catch( Exception e ) { 
				log.error( "failure in pool timeout thread", e );
			}
		}
	}
	
	public S3Stream get( String bucket, String key, long offset, long length )
	{
		String poolKey = bucket + key;
		
		try {
			List<S3Stream> streams = pool.get( poolKey );
			if( streams == null )
			{
				streams = new ArrayList<>();
				pool.put( poolKey, streams );
			}
			
			log.debug( "searching for stream to file: " + streams.size() );
			
			// search for current stream for this request
			for( S3Stream stream : streams )
			{
				if( stream.within( offset ) )
					return stream;
			}
			
			log.debug( "creating new stream: " + streams.size() );
			
			S3Stream stream = new S3Stream(bucket, key, offset);
			streams.add( stream );
			return stream;
			
		} catch( Exception e ) {
			log.error( "failed to create S3Stream within S3StreamPool: ", e );
		}
		
		return null;
	}
}
