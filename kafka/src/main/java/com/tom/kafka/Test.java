package com.tom.kafka;

import java.util.Scanner;

/**
 * ClassName: Test
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/25 1:37
 */
public class Test {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        StringBuffer sb1 = new StringBuffer(sc.next());
        StringBuffer sb2 = new StringBuffer(sc.next());
        String[] arr1 = fun(sb1);
        String[] arr2 = fun(sb2);
        for(String s:arr1){
            System.out.println(s);
        }
        for(String s:arr2){
            System.out.println(s);
        }
    }
    public static String[] fun(StringBuffer sb1){
        String[] sArr = null;
        if (sb1.length()%8 == 0){
            sArr = new String[sb1.length()/8];
        }else {
            sArr = new String[sb1.length()/8+1];
        }

        int i = 0;
        int start = 0;
        int end = 8;
        System.out.println(sb1.length());
        while(true){

            if(sb1.length()>8){
                String str = sb1.substring(start,end);
                sArr[i] = str;
                i++;
                sb1= sb1.delete(start,end);
            }else{
                int len = sb1.length();
                for(int j=0;j<8-len;j++){
                    sb1.append("0");
                }
                String s = sb1.toString();
                sArr[i]=s;
                break;
            }
        }
        return sArr;
    }
}
