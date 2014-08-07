package com.shgc.otrack.dataclean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;



public class StringPair implements WritableComparable<StringPair>
{
	String macDevice;
    String macCaptured;
    
    
    public void set(String devMac, String capMac)
    {
    	macDevice = devMac;
    	macCaptured = capMac;
    }
    
    public String getMacDevice() {
		return macDevice;
	}

	public void setMacDevice(String macDevice) {
		this.macDevice = macDevice;
	}

	public String getMacCaptured() {
		return macCaptured;
	}

	public void setMacCaptured(String macCaptured) {
		this.macCaptured = macCaptured;
	}

	//反序列化，从流中的二进制转换成StringPair
    public void readFields(DataInput in) throws IOException
    {
    	macDevice = in.readUTF();
    	macCaptured = in.readUTF();
    }
    //序列化，将StringPair转化成使用流传送的二进制
    public void write(DataOutput out) throws IOException
    {
        out.writeUTF(macDevice);
        out.writeUTF(macCaptured);
    }
    //重载 compareTo 方法，进行组合键 key 的比较，该过程是默认行为。
    //分组后的二次排序会隐式调用该方法。
    public int compareTo(StringPair strPa)
    {
    	//System.out.println("StringPair compareTo called");
    	
        if (!macDevice.equals(strPa.macDevice) )
        {
            return macDevice.compareTo(strPa.macDevice);
        }
        else if (!macCaptured.equals(strPa.macCaptured))
        {
            return macCaptured.compareTo(strPa.macCaptured);
        }
        else
        {
            return 0;
        }
    }

    //新定义类应该重写的两个方法
    //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
    public int hashCode()
    {
        return macDevice.hashCode() * 157 + macCaptured.hashCode();
    }
    public boolean equals(Object strPa)
    {
        if (strPa == null)
            return false;
        if (this == strPa)
            return true;
        if (strPa instanceof StringPair)
        {
        	StringPair sPa = (StringPair) strPa;
            return sPa.macDevice.equals(macDevice) && sPa.macCaptured.equals(macCaptured) ;
        }
        else
        {
            return false;
        }
    }

}
 