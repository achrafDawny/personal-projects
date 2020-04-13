package core;

public interface MyInterface2 {
    default String helloDefault(){
        return "inside default method in interface";
    }
}
