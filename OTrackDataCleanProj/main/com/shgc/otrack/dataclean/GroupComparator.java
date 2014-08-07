package com.shgc.otrack.dataclean;

import org.apache.hadoop.io.WritableComparator;



/**
 * 分组函数类。只要first相同就属于同一个组。
 */
/*//第一种方法，实现接口RawComparator
public static class GroupComparator implements RawComparator<StringPair> {
    public int compare(StringPair o1, StringPair o2) {
        int l = o1.getFirst();
        int r = o2.getFirst();
        return l == r ? 0 : (l < r ? -1 : 1);
    }
    //一个字节一个字节的比，直到找到一个不相同的字节，然后比这个字节的大小作为两个字节流的大小比较结果。
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
         return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8,
                 b2, s2, Integer.SIZE/8);
    }
}*/
//第二种方法，继承WritableComparator
public class GroupComparator extends WritableComparator
{
    
    public GroupComparator() {
		super(StringPair.class, true);
		System.out.println("GroupComparator created");
	}

	//Compare two WritableComparables.
    //  重载 compare：对组合键按第一个自然键排序分组
	@Override
	public int compare(Object sp1, Object sp2) {
		//System.out.println("GroupComparator compare called");
		StringPair strPa1 = (StringPair) sp1;
        StringPair strPa2 = (StringPair) sp2;
        if(0 == strPa1.getMacDevice().compareTo(strPa2.getMacDevice()))
        {
	        return strPa1.getMacCaptured().compareTo(strPa2.getMacCaptured());
        }
        else
        {
        	return strPa1.getMacDevice().compareTo(strPa2.getMacDevice());
        }
	}
}
