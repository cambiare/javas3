package javaS3;

public class S3FileCache implements Comparable<Long>
{
	private Long offset;
	private byte[] cacheData;
	
	
	public Long getOffset() {
		return offset;
	}
	
	public void setOffset(Long offset) {
		this.offset = offset;
	}
	
	public byte[] getCacheData() {
		return cacheData;
	}
	
	public void setCacheData(byte[] cacheData) {
		this.cacheData = cacheData;
	}
	
	public boolean withinCache( long offsetQuery )
	{
		return offsetQuery >= offset && offsetQuery < offset + cacheData.length;
	}
	
	@Override
	public int compareTo( Long offset2 ) 
	{		
		if( withinCache( offset2 ) )
			return 0;
		
		return offset.compareTo( offset2 );
	}
	
}
