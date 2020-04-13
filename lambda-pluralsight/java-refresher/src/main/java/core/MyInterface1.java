package core;

public interface MyInterface1 {
    static String hello(){
        return "inside static method in interface";
    }

    default String helloDefault(){
        return "inside default method in interface";
    }
    void absmethod();
}
