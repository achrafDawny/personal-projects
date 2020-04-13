package demo;

import core.MyGeneric;
import core.MyInterface1;

public class MyGenericDemo {
    public static void main(String[] args) {
        MyGeneric<Integer> m1 = new MyGeneric<>(1);
        System.out.println(m1);
        System.out.println(m1.getInput());
        MyGeneric<String> m2 = new MyGeneric<>("hello");
        System.out.println(m2);
        System.out.println(m2.getInput());
        MyInterface1.hello();
    }
}
