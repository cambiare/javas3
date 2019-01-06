package javaS3;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.amazonaws.services.s3.model.S3ObjectIdBuilder;

interface CodeTimer {
	void timedExecution( );
}

public class Utils 
{
	private static final Logger log = Logger.getLogger( Utils.class );

	private static AmazonS3 s3 = getS3Client( );
	
	private Map<String, Long> timers = new HashMap<>();
	private static Map<String, S3ObjectId> objectIdMap = new HashMap<>();
	
	public static AmazonS3 getS3Client( )
	{
		if( s3 == null )
		{
			ClientConfiguration config = new ClientConfiguration();
			config.setMaxConnections( getProperty( "javas3.s3_max_connections", 1000 ) );
			config.setProxyDomain( Utils.getProperty( "javas3.proxy_domain", null ) );
			s3 = AmazonS3ClientBuilder.standard()
					.withClientConfiguration( config )
					.withRegion( getProperty( "javas3.s3_region", "us-east-1" ) )
					.build();
		}
		return s3;
	}
	
	public static int getProperty( String property, int defaultValue )
	{
		int returnValue = -1;
		
		String value = System.getProperty( property );
		if( value != null )
			returnValue = Integer.parseInt( value );
		else
			returnValue = defaultValue;
		
		log.warn( "int property available: " + property + " set to: " + returnValue );
		
		return returnValue;
	}
	
	public static S3ObjectId getObjectId( String bucket, String key )
	{
		
		S3ObjectId objid = new S3ObjectIdBuilder().withBucket( bucket ).withKey( key ).build();
		return objectIdMap.get( bucket + key );
	}
	
	public static String getProperty( String property, String defaultValue )
	{
		String returnValue = null;
		
		String value = System.getProperty( property );
		if( value != null )
			returnValue = value;
		else
			returnValue = defaultValue;
		
		log.warn( "String property available: " + property + " set to: " + returnValue );
		
		return returnValue;
	}
	
	public void startTimer( String name )
	{
		timers.put( name, System.currentTimeMillis() );
	}
	
	public void stopTimer( String name )
	{
		Long t2 = System.currentTimeMillis();
		Long t1 = timers.get( name );
		
		log.warn( "TIMER: " + name + ": " + (t2-t1) );
	}
	
}
