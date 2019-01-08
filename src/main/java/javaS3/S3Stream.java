package javaS3;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Stream 
{
	private static final Logger log = Logger.getLogger( S3Stream.class );
	
	private final static List<BufferBlock> buffers = Collections.synchronizedList( new LinkedList<>() );
	
	private boolean closed = false;
	
	//private Utils utils = new Utils();
	
	final static AmazonS3 	s3 = Utils.getS3Client();
	
	private long lastReadTime = System.currentTimeMillis();
	
	private AtomicLong offset;
	private AtomicLong maxReadLocation;
	private String bucket;
	private String key;
	
	public S3Stream( String bucket, String key, long offset )
	{
		this.bucket = bucket;
		this.key = key;
		
		this.offset = new AtomicLong( offset );
		this.maxReadLocation = new AtomicLong( offset );

		//fillFullFileBuffer( );
		fillBuffers();
	}
	
	public long getMaxBufferOffset( )
	{
		return (buffers.size() > 0 ) ? buffers.get( buffers.size() - 1 ).maxOffset() : (offset.get() + READ_AHEAD_SIZE);
	}
	
	public long getMinBufferOffset( )
	{
		return (buffers.size() > 0 ) ? buffers.get( 0 ).getOffset() : offset.get();
	}
	
	public long getOffset( )
	{
		return offset.get();
	}
	
	public long getLastReadTime( )
	{
		return lastReadTime;
	}
	
	public boolean isClosed()
	{		
		return closed && buffers.size() <= 0;
	}
	
	public boolean within( long requestOffset )
	{
		return requestOffset >= getMinBufferOffset() && requestOffset <= getMaxBufferOffset() + READ_AHEAD_SIZE;
	}
	
	
	
	public byte[] read( long offset, long length ) throws TimeoutException
	{
		if( isClosed() )
			return null;
		
		// update the max read location for buffers to fill to that point if this is farther
		// than the rest
		//String msg = "updating maxReadLocation from: " + maxReadLocation + " to: ";
		long max = maxReadLocation.updateAndGet( x -> Math.max( offset+length, maxReadLocation.get() ) );
		if( max == offset+length )
		{
			//log.info( msg + maxReadLocation );
			synchronized( bufferFillMonitor ) { bufferFillMonitor.notify(); }
		}
		log.debug( "filling buffer" );
		
		int bufferWaitTTL = 0;
		byte[] buffer = null;
		while( (buffer = fillBufferFromBlockBuffers( offset, length ) ) == null )
		{
			log.debug( "not filled... waiting" );
			// notify for a buffer fill
			synchronized( bufferFillMonitor ) { bufferFillMonitor.notify(); }
			
			try { 
				synchronized( readMonitor ) {
					readMonitor.wait( 2 * 1000 );
				}
			} catch (InterruptedException e) { log.error( "interrupted in read wait", e ); }
			
			log.debug( "woke up readMonitor" );
			if( bufferWaitTTL++ >= 5 )
			{
				log.error( "timed out while waiting for buffers to fill" );
				throw new TimeoutException();
			}
		}
		
		lastReadTime = System.currentTimeMillis();

		return buffer;
	}
	
	public void close( )
	{
			closed = true;
	}
}
