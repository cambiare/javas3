package javaS3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Stream 
{
	private static final Logger log = Logger.getLogger( S3Stream.class );

	private final int IO_BUFFER_SIZE = 256 * 1024; // 256KB
	
	private boolean closed = false;
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private boolean locked = false;
	private long lastReadTime = System.currentTimeMillis();
	
	private BufferedInputStream bufferedStream;
	private AtomicLong offset;
	//private BlockingQueue<Byte> streamBuffer;
	
	public S3Stream( String bucket, String key, long offset )
	{
		this.offset = new AtomicLong( offset );
		
		GetObjectRequest request = new GetObjectRequest(bucket, key).withRange( offset );
		S3Object o = s3.getObject( request );
		
		bufferedStream = new BufferedInputStream( o.getObjectContent(), IO_BUFFER_SIZE );
		
		//streamBuffer = new ArrayBlockingQueue<>( 1 * 1024 * 1024 );
	}
	
	public boolean skip( long skip )
	{
		log.info( "advancing stream: " + skip );
		try {
			bufferedStream.skip( skip );
			return true;
		} catch (IOException e) {
			log.error( "failed to skip bytes in stream", e );
		}
		return false;
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
		return closed;
	}
	
	public int read( ) throws IOException
	{
		lastReadTime = System.currentTimeMillis();
		offset.incrementAndGet();
		int b = bufferedStream.read();
		if( b == -1 )
			close();
		
		return b;
		/*
		byte b = streamBuffer.take();
		offset++;
		return b;
		*/
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
