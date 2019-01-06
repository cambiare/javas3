package javaS3;

import org.apache.log4j.Logger;

public class Utils 
{
	private static final Logger log = Logger.getLogger( Utils.class );

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
}
