package javaS3;

import java.io.BufferedInputStream;
import java.io.IOException;
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
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Stream 
{
	private static final Logger log = Logger.getLogger( S3Stream.class );

	private final int IO_BUFFER_SIZE = 1024*32; // 32KB
	private final int STREAM_BUFFER_BLOCK_SIZE = 256 * 1024; // 128KB
	private final int BUFFER_TIMEOUT = 1000 * 3; // 3 second buffer timeout
	private final int READ_AHEAD_SIZE = 1024 * 1024; // 1MB read ahead
	
	private final Object bufferFillMonitor = new Object();
	private final Object readMonitor = new Object();
	
	private final List<BufferBlock> buffers = Collections.synchronizedList( new LinkedList<>() );
	
	private boolean closed = false;
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private long lastReadTime = System.currentTimeMillis();
	
	private BufferedInputStream bufferedStream;
	private AtomicLong offset;
	private AtomicLong maxReadLocation;
	
	public S3Stream( String bucket, String key, long offset )
	{
		this.offset = new AtomicLong( offset );
		this.maxReadLocation = new AtomicLong( offset );
		
		GetObjectRequest request = new GetObjectRequest(bucket, key).withRange( offset );
		S3Object o = s3.getObject( request );
		
		bufferedStream = new BufferedInputStream( o.getObjectContent(), IO_BUFFER_SIZE );
		
		fillBuffers();
	}
	
	private void fillBuffers( )
	{
		Executors.newSingleThreadExecutor().execute( ()-> {
			try {
				boolean firstRun = true;
				byte[] buffer;
				
				int bytesRead = -1;
				while( !closed )
				{
					if( firstRun ) firstRun = false;
					else
						synchronized( bufferFillMonitor ) {
							bufferFillMonitor.wait();
						}
					
					while( !closed && buffers.size() <= 0 || 
						  (buffers.get( buffers.size() -1 ).maxOffset() - maxReadLocation.get()) < READ_AHEAD_SIZE )
					{
						buffer = new byte[STREAM_BUFFER_BLOCK_SIZE];
						bytesRead = bufferedStream.read( buffer );
						if( bytesRead < STREAM_BUFFER_BLOCK_SIZE )
						{
							if( bytesRead < 0 )
								closed = true;
							else if( bytesRead > 0 )
								buffer = Arrays.copyOf( buffer, bytesRead );
							else {
								// zero bytes returned - this shouldn't happen
								Thread.sleep( 10 );
								continue;
							}
						}
						
						synchronized( buffers )
						{
							buffers.add( new BufferBlock( buffer, offset.getAndAdd( bytesRead ) ) );
						}
						
						log.info( "filled buffer: " + bytesRead + " - " + offset + " - " + buffers.get( buffers.size() -1 ).maxOffset() + " - " + maxReadLocation.get() );

						clearBuffers();
						
						synchronized( readMonitor ) {
							readMonitor.notifyAll();
						}
						
						if( bytesRead < 0 )
							break;
					}
				}
				
				closed = true;
				log.info( "exiting buffer thread" );
			} catch( Exception e ) {
				log.error( "buffer thread has failed ", e );
				closed = true;
			}
		});
	}
	
	public long getOffset( )
	{
		return offset.get();
	}
	
	public long getLastReadTime( )
	{
		return lastReadTime;
	}
	
	private void clearBuffers( )
	{
		
		List<BufferBlock> deleteList = new ArrayList<>();
		for( BufferBlock buffer : buffers )
		{
			if( buffer.getLastAccessTime() < (System.currentTimeMillis() - BUFFER_TIMEOUT) )
			{
				log.info( "removed buffer" );
				deleteList.add( buffer );
			}
		}
		
		synchronized( buffers )
		{
			for( BufferBlock buffer : deleteList )
				buffers.remove( buffer );
		}
	}
	
	public boolean isClosed()
	{		
		return closed && buffers.size() <= 0;
	}
	
	public boolean within( long offset )
	{
		// maybe buffers haven't been filled yet
		if( buffers.size() <= 0 )
			return offset > this.offset.get() && offset < this.offset.get() + READ_AHEAD_SIZE;
		
		
		if( offset > buffers.get(0).getOffset() && offset < maxReadLocation.get() + READ_AHEAD_SIZE )
			return true;
		
		return false;
	}
	
	private BufferBlock findBufferForLocation( long location )
	{
		synchronized( buffers )
		{
			for( BufferBlock buffer : buffers )
			{				
				if( buffer.within( location ) )
					return buffer;
			}
		}
		return null;
	}
	
	public int read( long location ) throws TimeoutException
	{
		if( isClosed() )
			return -1;

		BufferBlock buffer = null;
		int bufferWaitTTL = 0;
		while( (buffer = findBufferForLocation( location ) ) == null )
		{
			synchronized( bufferFillMonitor )
			{
				bufferFillMonitor.notify();
			}
			
			try { 
				synchronized( readMonitor ) {
					readMonitor.wait( 2 * 1000 );
				}
			} catch (InterruptedException e) { log.error( "interrupted in read wait", e ); }
			
			log.info( "woke up readMonitor" );
			if( bufferWaitTTL++ >= 5 )
			{
				log.error( "timed out while waiting for buffers to fill" );
				throw new TimeoutException();
			}
		}		
		
		int b = Byte.toUnsignedInt( buffer.read(location) );
		
		//long updatedLocation = 
		maxReadLocation.updateAndGet( x -> Math.max( location, maxReadLocation.get() ) );
//		if( updatedLocation == location && buffers.size() > 0 && buffers.get(buffers.size()-1).maxLocation() > 0 )
//			bufferFillLock.notify();
		
		lastReadTime = System.currentTimeMillis();
		
		return b;
	}
	
	public void close( )
	{
		try {
			bufferedStream.close();
			closed = true;
		} catch (IOException e) {
			log.error( "failed to close S3Stream", e );
		}
	}
	
	class BufferBlock
	{
		private long offset;
		private long lastAccessTime;
		private final byte[] buffer;
		
		public BufferBlock( byte[] buffer, long offset )
		{
			this.buffer = buffer;
			this.offset = offset;
			lastAccessTime = System.currentTimeMillis();
		}
		
		public boolean within( long location )
		{
			return (location - this.offset) < buffer.length;
		}
		
		public long getOffset( )
		{
			return this.offset;
		}
		
		public byte read( long location )
		{
			lastAccessTime = System.currentTimeMillis();
			return buffer[(int)(location - this.offset)];
		}
		
		public long getLastAccessTime( )
		{
			return lastAccessTime;
		}
		
		public long maxOffset( )
		{
			return this.offset + buffer.length;
		}
	}
}
