package javaS3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
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
	private final int STREAM_BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
	
	private boolean closed = false;
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private boolean locked = false;
	private long lastReadTime = System.currentTimeMillis();
	
	private BufferedInputStream bufferedStream;
	private AtomicLong offset;
	private BlockingQueue<Byte> streamBuffer;
	
	public S3Stream( String bucket, String key, long offset )
	{
		this.offset = new AtomicLong( offset );
		
		GetObjectRequest request = new GetObjectRequest(bucket, key).withRange( offset );
		S3Object o = s3.getObject( request );
		
		bufferedStream = new BufferedInputStream( o.getObjectContent(), IO_BUFFER_SIZE );
		streamBuffer = new ArrayBlockingQueue<>( STREAM_BUFFER_SIZE );
		
		startBufferThread();
	}
	
	private void startBufferThread( )
	{
		Executors.newSingleThreadExecutor().execute( ()-> {
			try {
				int b = -1;
				while( (b = bufferedStream.read() ) != -1 )
					streamBuffer.put( (byte)b );
				
				closed = true;
			} catch( Exception e ) {
				log.error( "buffer thread has failed ", e );
				closed = true;
			}
		});
	}
	
	public boolean skip( long skip )
	{
		log.info( "advancing stream: " + skip );
		
		if( skip > streamBuffer.size() )
		{
			log.info( "rejecting skip request: " + skip );
			return false;
		}
		
		try {
			for( int i=0; i < skip; i++ )
				streamBuffer.take();
		} catch (InterruptedException e) {
			log.error( "skip attempt failed - stream is dead - closing", e );
			closed = true;
			return false;
		}
		
		return true;
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
		return closed && streamBuffer.size() <= 0;
	}
	
	public int read( ) throws IOException
	{
		if( closed && streamBuffer.size() <= 0 )
			return -1;
		
		lastReadTime = System.currentTimeMillis();
		
		byte b;
		try {
			b = streamBuffer.take();
			offset.incrementAndGet();
			return (int)b;
		} catch (InterruptedException e) {
			log.error( "interrupted during streamBuffer take", e );
			closed = true;
			return -1;
		}
	}
	
	public void close( )
	{
		try {
			streamBuffer = null;
			bufferedStream.close();
			closed = true;
		} catch (IOException e) {
			log.error( "failed to close S3Stream", e );
		}
	}
	
	public boolean isLocked( )
	{
		return locked;
	}
	
	public boolean lock( )
	{		
		if( !locked )
		{
			locked = true;
			return true;
		}
		return false;
	}
	
	public void unlock( )
	{
		locked = false;
	}
}
