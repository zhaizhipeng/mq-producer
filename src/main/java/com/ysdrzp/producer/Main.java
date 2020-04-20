package com.ysdrzp.producer;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring 启动类
 */
public class Main {

    public static void main(String[] args) {
        ApplicationContext ac = new ClassPathXmlApplicationContext("applicationContext.xml");
        TestSpring testSpring = (TestSpring) ac.getBean("userService");
        testSpring.test();
    }

}
