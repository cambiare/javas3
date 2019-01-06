package javaS3;

import java.nio.file.Paths;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import jnr.ffi.Pointer;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

public class FuseCallbackImpl extends FuseStubFS
{
	final AmazonS3 s3 = Utils.getS3Client();
	
	private static Logger log = Logger.getLogger( FuseCallbackImpl.class );
	
	private String bucket;

	private String getKey( String path )
	{
		return path.replaceFirst( "^/", "" );
	}
	
	@Override
	public int getattr(String path, FileStat stat)
	{
		S3FileStream file = S3FileStream.getFile( bucket, path );
		
		if( file.isDir() )
		{
			stat.st_mode.set(FileStat.S_IFDIR | 0555);
            stat.st_nlink.set(2);
		} else {
			stat.st_mode.set(FileStat.S_IFREG | 0444);
	        stat.st_nlink.set(1);
	        stat.st_size.set( file.getLength() );
		}
	    
        return 0;
	}

	@Override
	public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) 
	{
		//log.info( "read: " + size + " - " + offset );
		S3FileStream file = S3FileStream.getFile( bucket, path );
		
		byte[] buffer = file.read( offset, size );
		buf.put(0, buffer, 0, (int)buffer.length);
		
		return buffer.length;
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
        FuseCallbackImpl stub = new FuseCallbackImpl();
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
