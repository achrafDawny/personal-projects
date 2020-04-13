package core;

public class MyGeneric<T> {
    T input;
    public MyGeneric(T input){
        this.input = input;
    }

    public T getInput(){
        return input;
    }

    @Override
    public String toString() {
        return "MyGeneric{" +
                "input=" + input +
                '}';
    }
}
