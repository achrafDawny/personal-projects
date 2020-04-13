package demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LambdaDemo {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1,2,3,4,5);
        list.forEach(n -> System.out.println(n));
        list.stream().map(n -> n*2).forEach(n -> System.out.println(n));
    }
}
