package com.bigdata.mapreduce.orderSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ItemIdGroupComparator extends WritableComparator {

    //传入作为key的bean的class类型，以及制定需要让框架做反射获取实例对象
    protected ItemIdGroupComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;

        //比较两个bean时，指定只比较bean中的orderid
        return aBean.getItemId().compareTo(bBean.getItemId());
    }
}
