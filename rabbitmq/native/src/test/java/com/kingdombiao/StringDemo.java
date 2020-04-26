package com.kingdombiao;

/**
 * 描述:
 * ${DESCRIPTION}
 *
 * @author biao
 * @create 2019-09-06 9:44
 */
public class StringDemo {

    public static void main(String[] args) {
        String a="abc";

        System.out.println(a.hashCode());

        a="bed";
        System.out.println(a.hashCode());

        Integer b=1;
        System.out.println(b.hashCode());
        b=2;
        System.out.println(b.hashCode());

    }
}
