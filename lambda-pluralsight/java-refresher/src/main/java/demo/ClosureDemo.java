package demo;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ClosureDemo {
   public static void main(String[] args) { 
      List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
      Function<Integer, Integer> closure = ClosureExample.closure(2);
      list.stream().map(closure).forEach(n -> System.out.print(n+" "));
      System.out.println();
      int a = 2;
      list.stream().map(closure).forEach(n -> System.out.print(n+" "));
   } 
}

class ClosureExample {
   public static Function<Integer, Integer> closure(int m) {
      int a=m;
      Function<Integer, Integer> function = t->{
         return t*a; // a is available to be accessed in this function
      };
      return function;
   }
}