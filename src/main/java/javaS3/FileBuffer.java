package javaS3;

import java.io.BufferedInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class FileBuffer 
{
	private final static int IO_BUFFER_SIZE = Utils.getProperty( "javas3.io_buffer_size", 1024*32 ); // 32KB
	private final static int STREAM_BUFFER_BLOCK_SIZE = Utils.getProperty( "javas3.buffer_block_size", 256 * 1024 ); // 128KB
	private final static int BUFFER_TIMEOUT = Utils.getProperty( "javas3.buffer_timeout", 1000 * 3 ); // 3 second buffer timeout
	private final static int READ_AHEAD_SIZE = Utils.getProperty( "javas3.read_ahead_size", 1024 * 1024 ); // 1MB read ahead
	//private final static int MAX_READ_FROM_STREAM = Utils.getProperty( "javas3.max_read_from_stream", 2 * 1024 * 1024 ); // 2MB read ahead

	final static AmazonS3 	s3 = Utils.getS3Client();
	
	private static final Logger log = Logger.getLogger( FileBuffer.class );
	
	private final static List<BufferBlock> buffers = Collections.synchronizedList( new LinkedList<>() );

	public static final Object bufferFillMonitor = new Object();
	public static final Object readMonitor = new Object();
	
	private int fillBuffer( String bucket, String key, byte[] buffer, long offset )
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
	
	private void fillFullFileBuffer( String bucket, String key )
	{
		int bytesRead = -1;
		
		GetObjectRequest request = 
				new GetObjectRequest( bucket, key );
		
		S3Object s3object = s3.getObject( request );
		
		// if 100MB or greater then use the partial buffer method
		if( s3object.getObjectMetadata().getContentLength() > 100 * 1024 * 1024 )
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
			
			log.info( "filled full buffer: " + bytesRead );
			
			BufferBlock bufferBlock = new BufferBlock( buffer, 0l );
			buffers.add( bufferBlock );
			
			bufferedStream.close();
		} catch (Exception e) {
			log.error( "failed to read buffer: ", e );
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
				if( firstRun ) firstRun = false;
				else
					synchronized( bufferFillMonitor ) {
						bufferFillMonitor.wait();
					}
				
				//utils.startTimer( "fillAllBuffers" );
				while( buffers.size() <= 0 || 
					  (buffers.get( buffers.size() -1 ).maxOffset() - maxReadLocation.get()) < READ_AHEAD_SIZE )
				{
					buffer = new byte[STREAM_BUFFER_BLOCK_SIZE];
					
					bytesRead = fillBuffer( buffer, offset.get() );
					//totalBytesRead += bytesRead;
					
					if( bytesRead < STREAM_BUFFER_BLOCK_SIZE )
					{
						if( bytesRead > 0 )
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
				
				log.debug( "exiting buffer thread" );
			} catch( Exception e ) {
				log.error( "buffer thread has failed ", e );
			}
		});
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
	
	public byte[] fillBufferFromBlockBuffers( long offset, long length ) 
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
		return returnData;
	}
}
