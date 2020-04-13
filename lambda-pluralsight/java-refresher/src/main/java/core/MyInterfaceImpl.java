package core;

public class MyInterfaceImpl implements MyInterface1, MyInterface2 {

    // we need to override default method helloDefault
    // because MyInterface1 and MyInterface2 contains a default
    // method with the same name
    @Override
    public String helloDefault() {
        return "inside the implementation of the default method in MyInterfaceImpl";
    }

    @Override
    public void absmethod() {
        System.out.println("Abstract method implementation in class");
    }

}
