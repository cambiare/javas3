package javaS3;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Stream 
{
	private final int IO_BUFFER_SIZE = 32 * 1024; // 32KB
	
	//private final 
	
	final static AmazonS3 	s3;
	static {
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	
	private boolean locked;
	private long lastReadTime = System.currentTimeMillis();
	
	private BufferedInputStream bufferedStream;
	private long offset;
	//private BlockingQueue<Byte> streamBuffer;
	
	public S3Stream( String bucket, String key, long offset )
	{
		this.offset = offset;
		
		GetObjectRequest request = new GetObjectRequest(bucket, key).withRange( offset );
		S3Object o = s3.getObject( request );
		
		bufferedStream = new BufferedInputStream( o.getObjectContent(), IO_BUFFER_SIZE );
		
		//streamBuffer = new ArrayBlockingQueue<>( 1 * 1024 * 1024 );
	}
	
	public long getOffset( )
	{
		return offset;
	}
	
	public int read( ) throws IOException
	{
		lastReadTime = System.currentTimeMillis();
		offset++;
		return bufferedStream.read();
		/*
		byte b = streamBuffer.take();
		offset++;
		return b;
		*/
	}
	
	public boolean isLocked( )
	{
		return locked;
	}
	
	public void lock( )
	{
		locked = true;
	}
	
	public void unlock( )
	{
		locked = false;
	}
}
