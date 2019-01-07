package javaS3;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
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
	
	public static AmazonS3 getS3Client( )
	{
		if( s3 == null )
		{		
			EndpointConfiguration endpointConfig = null;
			
			String endpoint = getProperty( "javas3.s3_endpoint", null );
			
			ClientConfiguration config = new ClientConfiguration();
			config.setMaxConnections( getProperty( "javas3.s3_max_connections", 1000 ) );
			config.setProxyDomain( Utils.getProperty( "javas3.proxy_domain", null ) );
			config.setProxyDomain( Utils.getProperty( "javas3.proxy_port", null ) );
			
			if( endpoint != null )
			{
				endpointConfig = new  EndpointConfiguration( endpoint, getProperty( "javas3.s3_region", "us-east-1" ) );
				s3 = AmazonS3ClientBuilder.standard()
						.withClientConfiguration( config )
						.withEndpointConfiguration( endpointConfig )
						.build();
			} else {
				s3 = AmazonS3ClientBuilder.standard()
						.withClientConfiguration( config )
						.withRegion( getProperty( "javas3.s3_region", "us-east-1" ) )
						.build();
			}
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
