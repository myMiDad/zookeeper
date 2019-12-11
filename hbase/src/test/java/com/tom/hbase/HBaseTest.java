package com.tom.hbase;

import org.junit.Test;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * ClassName: HBaseTest
 * Description:
 *
 * @author Mi_dad
 * @date 2019/11/19 22:23
 */
public class HBaseTest {

    @Test
    public void existsTest() throws IOException {
        boolean result = HBaseApi.isTableExist("student");
        System.out.println(result);
    }

    @Test
    public void createTable() throws IOException {
        HBaseApi.createTable("student3", "info");
        boolean result = HBaseApi.isTableExist("student3");
        System.out.println(result);
    }

    @Test
    public void test() throws NoSuchAlgorithmException {
        String str = getStr("root");
        System.out.println(str);
    }

    public String getStr(String str) throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(str.getBytes());
        byte[] result = md5.digest();
        return new String(result);

    }

}
