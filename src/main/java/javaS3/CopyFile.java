package javaS3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

public class CopyFile 
{
	public static void main( String args[] ) throws Exception
	{
		String from = "/tmp/test/1f770231dae43e99e9dd8cd119dc19c0/content/data/dynamic_mosaic/023001113320/023001113320.tif";
		String to = "/tmp/test.tif";
				
		InputStream in =  new FileInputStream( from   ) ;
		BufferedOutputStream out = new BufferedOutputStream( new FileOutputStream( to ) );
		
		int c = 0;
		while( (c = in.read()) > -1 )
		{
			out.write( c );
		}
		
		out.flush();
		in.close();
		out.close();
				
	}
}
