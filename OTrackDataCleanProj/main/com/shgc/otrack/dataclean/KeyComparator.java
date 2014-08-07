package com.shgc.otrack.dataclean;

import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator
{

	

	public KeyComparator() {
		super(StringPair.class, true);
		System.out.println("KeyComparator created");
	}

	@Override
	public int compare(Object sp1, Object sp2) {
		System.out.println("KeyComparator compare called");
		
		StringPair strPa1 = (StringPair) sp1;
        StringPair strPa2 = (StringPair) sp2;
        if(0 != strPa1.getMacDevice().compareTo(strPa2.getMacDevice()))
        {
	        return strPa1.getMacCaptured().compareTo(strPa2.getMacCaptured());
        }
        else
        {
        	return 0;
        }
	}
}
