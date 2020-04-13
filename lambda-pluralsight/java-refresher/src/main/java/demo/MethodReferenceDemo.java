package demo;

import core.Tools;

import java.util.stream.IntStream;

public class MethodReferenceDemo {
    public static void main(String[] args) {
        Tools t = new Tools();
        IntStream.range(1, 8).filter(Tools::isOdd).forEach(t::print);
    }
}
