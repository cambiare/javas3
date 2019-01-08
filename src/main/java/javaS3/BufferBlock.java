package javaS3;

public class BufferBlock 
{
	private long offset;
	private long lastAccessTime;
	private final byte[] buffer;
	
	public BufferBlock( byte[] buffer, long offset )
	{
		this.buffer = buffer;
		this.offset = offset;
		lastAccessTime = System.currentTimeMillis();
	}
	
	public boolean within( long location )
	{
		return (location - this.offset) < buffer.length;
	}
	
	public long getOffset( )
	{
		return this.offset;
	}
	
	public byte[] getBuffer( )
	{
		return buffer;
	}
	
	public byte read( long location )
	{
		lastAccessTime = System.currentTimeMillis();
		return buffer[(int)(location - this.offset)];
	}
	
	public long getLastAccessTime( )
	{
		return lastAccessTime;
	}
	
	public long maxOffset( )
	{
		return this.offset + buffer.length;
	}
}
