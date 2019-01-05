package javaS3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

public class S3StreamPool 
{
	private Map<String, List<S3Stream>>		pool = new HashMap<>();
	
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
			for( String key : pool.keySet() )
			{
				for( S3Stream stream : pool.get(key) )
				{
					if( (System.currentTimeMillis() - stream.getLastReadTime()) > 15000 )
					{
						stream.close();
						deleteList.add( stream );
					}
				}
				
				for( S3Stream stream : deleteList )
				{
					log.info( "removing timed out stream: " + key );
					pool.get(key).remove( stream );
				}
				
				deleteList.clear();
			}
			
			try {
				Thread.sleep( 1000 );
			} catch (InterruptedException e) {
				log.error( "interrupted in timeoutUnusedStreams", e );
			}
		}
	}
	
	public S3Stream get( String bucket, String key, Long offset )
	{
		try {
			List<S3Stream> streams = pool.get( bucket+key );
			if( streams == null )
			{
				streams = new ArrayList<>();
				pool.put( bucket+key, streams );
			}
			
			//log.debug( "searching for stream to file: " + streams.size() + " " + key );
			
			for( S3Stream stream : streams )
			{
				if( !stream.isClosed() && Math.abs( offset.longValue() - stream.getOffset() ) < (1024 * 1024) )
				{	
					return stream;
				}
			}
			
			log.info( "creating new stream: " + streams.size() );
			
			S3Stream stream = new S3Stream(bucket, key, offset);
			streams.add( stream );
	
			return stream;
		} catch( Exception e ) {
			log.error( "failed to create S3Stream within S3StreamPool: ", e );
		}
		
		return null;
	}
}
