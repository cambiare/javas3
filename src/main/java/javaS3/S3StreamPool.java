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
	
	public synchronized S3Stream get( String bucket, String key, long offset, long length )
	{
		String poolKey = bucket + key;
		
		try {
			List<S3Stream> streams = pool.get( poolKey );
			if( streams == null )
			{
				streams = new ArrayList<>();
				pool.put( poolKey, streams );
			}
			
			log.info( "searching for stream to file: " + streams.size() + " " + key );
			
			for( S3Stream stream : streams )
			{
				if( !stream.isLocked() && !stream.isClosed() )
				{
					if( offset == stream.getOffset() )
					{
						stream.lock();
						return stream;
					}
					
					long skip = offset - stream.getOffset();
					if( offset > stream.getOffset() && skip < (length*2) )
					{
						stream.lock();
						if( stream.skip( skip ) )
							return stream;
					}
				}
				
				log.info( "stream: " + stream.getOffset() + " --- search offset: " + offset );
				S3Stream lockedStream = getLockedStream( stream, offset );
				if( lockedStream != null ) return lockedStream;
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
	
	private synchronized S3Stream getLockedStream( S3Stream stream, Long offset )
	{
		if( !stream.isClosed() && offset.longValue() == stream.getOffset() && stream.lock() )
			return stream;
		
		return null;
	}
}
