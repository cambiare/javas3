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

	private final static int IO_BUFFER_SIZE = Utils.getProperty( "javas3.io_buffer_size", 1024*32 ); // 32KB
	private final static int STREAM_BUFFER_BLOCK_SIZE = Utils.getProperty( "javas3.buffer_block_size", 256 * 1024 ); // 128KB
	private final static int BUFFER_TIMEOUT = Utils.getProperty( "javas3.buffer_timeout", 1000 * 3 ); // 3 second buffer timeout
	private final static int READ_AHEAD_SIZE = Utils.getProperty( "javas3.read_ahead_size", 1024 * 1024 ); // 1MB read ahead
	//private final static int MAX_READ_FROM_STREAM = Utils.getProperty( "javas3.max_read_from_stream", 2 * 1024 * 1024 ); // 2MB read ahead
	
	private final Object bufferFillMonitor = new Object();
	private final Object readMonitor = new Object();
	
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
	
	private int fillBuffer( byte[] buffer, long offset )
	{
		//utils.startTimer( "FillBuffer" );
		int bytesRead = -1;
		
		GetObjectRequest request = 
				new GetObjectRequest(bucket, key).withRange( offset, offset + STREAM_BUFFER_BLOCK_SIZE );
		
		try( BufferedInputStream bufferedStream = 
				new BufferedInputStream( 
					s3.getObject( request ).getObjectContent(), IO_BUFFER_SIZE ) ) 
		{
			bytesRead = bufferedStream.read( buffer );
		
			bufferedStream.close();
		} catch (Exception e) {
			log.error( "failed to read buffer: " + offset, e );
		}
		//utils.stopTimer( "FillBuffer" );
		
		return bytesRead;
	}
	
	private void fillFullFileBuffer( )
	{
		int bytesRead = -1;
		
		GetObjectRequest request = 
				new GetObjectRequest(bucket, key);
		
		S3Object s3object = s3.getObject( request );
		
		// if 100MB or greater then use the partial buffer method
		if( s3object.getObjectMetadata().getContentLength() > 300 * 1024 * 1024 )
		{
			fillBuffers();
			return;
		}
		
		try( BufferedInputStream bufferedStream = 
				new BufferedInputStream( 
					s3.getObject( request ).getObjectContent(), IO_BUFFER_SIZE ) ) 
		{
			// only supporting sub 2GB here
			byte[] buffer = new byte[Math.toIntExact( s3object.getObjectMetadata().getContentLength() )];
			bytesRead = bufferedStream.read( buffer );
			
			log.info( "filled full buffer: " + bytesRead + ": " + key );
			
			BufferBlock bufferBlock = new BufferBlock( buffer, 0l );
			buffers.add( bufferBlock );
			
			bufferedStream.close();
		} catch (Exception e) {
			log.error( "failed to read buffer: " + offset, e );
		}
	}
	
	private void fillBuffers( )
	{
		Executors.newSingleThreadExecutor().execute( ()-> {
			try {
				boolean firstRun = true;
				byte[] buffer;
				
				//long totalBytesRead = 0;
				int bytesRead = -1;
				while( !closed )
				{
					if( firstRun ) firstRun = false;
					else
						synchronized( bufferFillMonitor ) {
							bufferFillMonitor.wait();
						}
					
					//utils.startTimer( "fillAllBuffers" );
					while( !closed && buffers.size() <= 0 || 
						  (buffers.get( buffers.size() -1 ).maxOffset() - maxReadLocation.get()) < READ_AHEAD_SIZE )
					{
						buffer = new byte[STREAM_BUFFER_BLOCK_SIZE];
						
						bytesRead = fillBuffer( buffer, offset.get() );
						//totalBytesRead += bytesRead;
						
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
						
						//log.debug( "filled buffer: " + bytesRead + " - " + totalBytesRead + " - " + offset + " - " + buffers.get( buffers.size() -1 ).maxOffset() + " - " + maxReadLocation.get() );

						clearBuffers();
						
						synchronized( readMonitor ) {
							readMonitor.notifyAll();
						}
						
						if( bytesRead < 0 )
						{
							break;
						}
					}
					//utils.stopTimer( "fillAllBuffers" );
				}
				
				closed = true;
				log.debug( "exiting buffer thread" );
			} catch( Exception e ) {
				log.error( "buffer thread has failed ", e );
				closed = true;
			}
		});
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
	
	private void clearBuffers( )
	{
		List<BufferBlock> deleteList = new ArrayList<>();
		for( BufferBlock buffer : buffers )
		{
			if( buffer.getLastAccessTime() < (System.currentTimeMillis() - BUFFER_TIMEOUT) )
			{
				log.debug( "removed buffer" );
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
	
	public boolean within( long requestOffset )
	{
		return true;
		//return requestOffset >= getMinBufferOffset() && requestOffset <= getMaxBufferOffset() + READ_AHEAD_SIZE;
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
			
			log.debug( "woke up readMonitor" );
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
	
	private byte[] fillBufferFromBlockBuffers( long offset, long length ) 
	{
		List<BufferBlock> bufferBlocksForFill = new ArrayList<>();
		
		log.debug( "entered fillBufferFromBlockBuffers" );
		
//		if( closed )
//			length = Math.min( this.offset.get() - offset, length );
		
		if( length <= 0 )
			return new byte[0];
		
		log.debug( "searching for buffers to fill this request" );
		long searchOffset = offset;
		for( int i=0; i < buffers.size(); i++ )
		{
			BufferBlock buffer = buffers.get( i );
			if( buffer.within( searchOffset ) )
			{
				bufferBlocksForFill.add( buffer );
				searchOffset = buffer.maxOffset() +1;
			}
			
			if( searchOffset > (offset+length) )
				break;
		}
		
		//log.debug( "found buffers: " + bufferBlocksForFill.size() );
		if( bufferBlocksForFill.size() <= 0 )
			return null;
		
		// do not have enough buffered to fill the request
		if( searchOffset < (offset+length) )
			return null;
		
		log.debug( "starting buffer fill loop" );
		
		int intLength = Math.toIntExact( length );
		byte[] returnData = new byte[intLength];
		int dataToRead = intLength;
		int destPos = 0;
		int srcPos = 0;
		for( BufferBlock buffer : bufferBlocksForFill )
		{
			long offsetDelta = offset - buffer.getOffset();
			
			srcPos = 0;
			if( offsetDelta >= 0 )
				srcPos = Math.toIntExact( offsetDelta );
			
			int srcLength = Math.min( buffer.getBuffer().length, dataToRead );
			srcLength = Math.min( returnData.length, srcLength );
			
			System.arraycopy( buffer.getBuffer(), srcPos, returnData, destPos, srcLength );
			
			dataToRead -= srcLength;
		}
		
		lastReadTime = System.currentTimeMillis();

		return returnData;
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
		
		return buffer;
	}
	
	public void close( )
	{
			closed = true;
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
		
		public byte[] getBuffer( )
		{
			return buffer;
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
