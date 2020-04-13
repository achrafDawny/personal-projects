package demo;

import core.MyInterface1;
import core.MyInterfaceImpl;

public class MyInterfaceDemo {
    public static void main(String[] args) {
        System.out.println(MyInterface1.hello());
        MyInterfaceImpl obj = new MyInterfaceImpl();
        //obj.hello(); won't compile
        System.out.println(obj.helloDefault());
    }
}
