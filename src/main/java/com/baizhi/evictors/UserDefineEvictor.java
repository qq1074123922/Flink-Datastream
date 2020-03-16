package com.baizhi.evictors;

import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

public class UserDefineEvictor implements Evictor<String, TimeWindow> {
    private Boolean isEvictorAfter=false;
    private String excludeContent=null;

    public UserDefineEvictor(Boolean isEvictorAfter, String excludeContent) {
        this.isEvictorAfter = isEvictorAfter;
        this.excludeContent = excludeContent;
    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<String>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        if(!isEvictorAfter){
            evict(elements,size,window,evictorContext);
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<String>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        if(isEvictorAfter){
            evict(elements,size,window,evictorContext);
        }
    }
    private void evict(Iterable<TimestampedValue<String>> elements, int size, TimeWindow window, EvictorContext evictorContext){
        for( Iterator<TimestampedValue<String>> iterator = elements.iterator();iterator.hasNext();){
            TimestampedValue<String> element = iterator.next();
            //将含有相关内容元素删除
            System.out.println(element.getValue());
            if(element.getValue().contains(excludeContent)){
                iterator.remove();
            }
        }
    }
}
