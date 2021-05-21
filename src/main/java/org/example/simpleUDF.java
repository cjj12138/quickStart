package org.example;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.*;

public class simpleUDF extends UDF {

    public Text evaluate(final Text s) {
        StringBuilder result= new StringBuilder();
        if (s == null) {
            return new Text("数据为空");
        }else {
            List<String[]> list=new ArrayList<>();
            String[] splits = s.toString().split("\\|");
            for (String value : splits) {
                String[] split = value.split(",");
                list.add(split);
            }
            String[] intersect = intersect(list.get(0), list.get(1));
            for (String val : intersect){
                if (result.toString().equals("")){
                    result.append(val);
                }else {
                    result.append(",").append(val);
                }
            }
            return new Text(result.toString()+"|"+intersect.length);
        }
    }

    /**
     * 下面部分是用于本地测试，在new Text（）中写如你想写的参数
     * @param args
     */
    public static void main(String[] args) {
        simpleUDF udf = new simpleUDF();
        Text result = udf.evaluate(new Text("a,b,c,d|d,b,e,t"));
        System.out.println(result.toString());
    }


    //并集（set唯一性）
    public static String[] union (String[] arr1, String[] arr2){
        Set<String> hs = new HashSet<String>();
        for(String str:arr1){
            hs.add(str);
        }
        for(String str:arr2){
            hs.add(str);
        }
        String[] result={};
        return hs.toArray(result);
    }

    //交集(注意结果集中若使用LinkedList添加，则需要判断是否包含该元素，否则其中会包含重复的元素)
    public static String[] intersect(String[] arr1, String[] arr2){
        List<String> l = new LinkedList<String>();
        Set<String> common = new HashSet<String>();
        for(String str:arr1){
            if(!l.contains(str)){
                l.add(str);
            }
        }
        for(String str:arr2){
            if(l.contains(str)){
                common.add(str);
            }
        }
        String[] result={};
        return common.toArray(result);
    }
    //求两个数组的差集
    public static String[] substract(String[] arr1, String[] arr2) {
        LinkedList<String> list = new LinkedList<String>();
        for (String str : arr1) {
            if(!list.contains(str)) {
                list.add(str);
            }
        }
        for (String str : arr2) {
            if (list.contains(str)) {
                list.remove(str);
            }
        }
        String[] result = {};
        return list.toArray(result);
    }
}
