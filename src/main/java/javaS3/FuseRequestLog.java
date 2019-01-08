package javaS3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;


public class FuseRequestLog implements Comparable<FuseRequestLog>
{
	private static Logger log = Logger.getLogger( FuseRequestLog.class );

	public static List<FuseRequestLog> requests = new ArrayList<>(10000);
	
	public String path;
	public Long offset;
	public long length;
	public long startTime;
	public long endTime;
	
	public FuseRequestLog( String path, long offset, long size, long startTime )
	{
		this.path = path;
		this.offset = offset;
		this.length = size;
		this.startTime = startTime;
	}
	
	static {
		Executors.newSingleThreadExecutor().execute( ()->{
			while(true)
			{
				logRequests();
				try {
					Thread.sleep( 1000 );
				} catch (InterruptedException e) {
					log.error( "exiting log thread", e );
					break;
				}
			}
		});
	}
	
	public static void logRequests( )
	{
		List<FuseRequestLog> batchRequests = requests;
		requests = new ArrayList<>(10000);
		
		Collections.sort( batchRequests );
		for( FuseRequestLog request : batchRequests )
		{
			if( request.endTime == 0 )
				requests.add( request );
			else
				log.info( 
					"REQUEST: " + 
					request.path.replaceAll( "^.*/", "") + ": " + 
					request.offset + "-" + (request.offset+request.length) + "-" + request.length + " --- " + 
					(request.endTime - request.startTime) );
		}
	}

	@Override
	public int compareTo(FuseRequestLog o) {
		
		int ret = this.path.compareTo( o.path );
		if( ret != 0 )
			return ret;
		
		return this.offset.compareTo( o.offset );
	}
}
