package javaS3;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheSdkHttpClientFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

interface CodeTimer {
	void timedExecution( );
}

public class Utils 
{
	private static final Logger log = Logger.getLogger( Utils.class );

	private static S3Client s3 = getS3Client( );
	
	private Map<String, Long> timers = new HashMap<>();
	
	public static S3Client getS3Client( )
	{
		if( s3 == null )
		{
			SdkHttpClient client = 
					ApacheSdkHttpClientFactory.builder()
						.maxConnections( getProperty( "javas3.s3_max_connections", 1000 ) )
						.build().createHttpClient();
			
			s3 = S3Client.builder()
				.httpClient( client )
				.endpointOverride( getProperty( "javas3.endpoint", null ) )
				.region( Region.of( getProperty( "javas3.s3_region", "us-east-1" ) ) )
				.build();
		}
		return s3;
	}
	
	public static <T> T getProperty( String property, T defaultValue )
	{
		T returnValue = null;
		
		String value = System.getProperty( property );
		if( value != null )
		{
			if( defaultValue instanceof Integer )
				returnValue = (T)new Integer(Integer.parseInt( value ));
			else if( defaultValue instanceof Long )
				returnValue = (T)new Long(Long.parseLong( value ));
			else if( defaultValue instanceof Float )
				returnValue = (T)new Float(Float.parseFloat( value ));
			else if( defaultValue instanceof Double )
				returnValue = (T)new Double(Double.parseDouble( value ));
			else
				returnValue = (T)value;
		} else
			returnValue = defaultValue;
		
		log.warn( "int property available: " + property + " set to: " + returnValue );
		
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
