package javaS3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class Driver extends FuseStubFS
{
	final AmazonS3 s3;
	
	Logger log = Logger.getLogger( Driver.class );
	
	private String bucket;

	public Driver( )
	{
		s3 = AmazonS3ClientBuilder.standard().withRegion( "us-east-1" ).build();
	}
	
	private String getKey( String path )
	{
		return path.replaceFirst( "^/", "" );
	}
	
	
	
	@Override
	public int getattr(String path, FileStat stat)
	{
		String key = getKey( path );
		log.info( "getattr called: " + key );
		
		if( key.isEmpty() || !key.contains(".") )
		{
			stat.st_mode.set(FileStat.S_IFDIR | 0555);
            stat.st_nlink.set(2);
			return 0;
		}
		
		log.info( "testing for key existence: " + key );
		ObjectMetadata meta = s3.getObjectMetadata( bucket, key );
		
		if( meta != null )
		{
			log.info( "found: " + meta.getContentLength() );
			stat.st_mode.set(FileStat.S_IFREG | 0444);
	        stat.st_nlink.set(1);
	        stat.st_size.set( meta.getContentLength() );
	        return 0;
		}
		
        return -ErrorCodes.ENOENT();
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) 
	{
		BufferedReader in;
		int bytesRead = 0;
		
		String key = getKey(path);
		
		log.info( "read called: " + key + " - " + size + " - " + offset );
		
		try {
			GetObjectRequest request;
			
			if( offset == 0 )
			{
				ObjectMetadata meta = s3.getObjectMetadata(bucket, key);
				if( meta != null )
				{
					long newsize = meta.getContentLength();
					log.info( "updating size from: " + size + " to: " + newsize );
					size = newsize;
					request = new GetObjectRequest(bucket, key);
				} else {
					return 0;
				}
			} else {
				request = new GetObjectRequest(bucket, key).withRange( offset, (offset+size-1) );
			}
			
			S3Object o = s3.getObject( request );
			in = new BufferedReader( new InputStreamReader( o.getObjectContent() ) );
			
			byte[] main_buf = new byte[(int)size];
			
			int c = -1;
		    while( (c = in.read()) >= 0 )
		    {
		    	if( bytesRead >= size )
		    		break;
		    	
		    	main_buf[bytesRead++] = (byte)c;
		    }
					
		    log.info( "bytesRead: " + bytesRead );
		    buf.put(0, main_buf, 0, (int)size);
		    in.close();
		} catch (Exception e) {
		    log.error(e);
		}
		
		return bytesRead;
	}
	
	@Override
	public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) 
	{
		String delimiter = "/";
		if( !path.endsWith(delimiter) )
		{
			path = path + delimiter;
		}
		
		String key = getKey(path);
		log.info( "readdir called: " + path );
		
		ListObjectsRequest lorequest = new ListObjectsRequest().withBucketName(bucket).withPrefix( key ).withDelimiter(delimiter);

		ObjectListing result = s3.listObjects( lorequest );
		
		for( String subkey : result.getCommonPrefixes() ) 
		{
			subkey = subkey.replaceAll("/$", "");
			String s3path = subkey.replaceAll("^.*" + delimiter, "");
			filter.apply( buf, s3path, null, 0 );
		}
		
		for( S3ObjectSummary summary: result.getObjectSummaries() )
		{
			String s3path = summary.getKey().replaceAll("^.*/", "" );
			filter.apply( buf, s3path, null, 0 );
		}
        
        return 0;		
	}

	public static void main(String[] args) 
	{
        Driver stub = new Driver();
        try {
        	if( args.length < 2 )
        	{
        		printUsage();
        		return;
        	}
        	
            stub.bucket = args[0];
            String mountPoint = args[1];
            
            String opts[] = null;
            if( args.length > 2 )
            	opts = args[2].split(",");
            
            stub.mount(Paths.get(mountPoint), true, false, opts);
        } finally {
            stub.umount();
        }
	}
	
	public static void printUsage( )
	{
		System.out.println( "USAGE: java -jar {JAR} bucket mount_point block fuse_opts" );
	}
}
