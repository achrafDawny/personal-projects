package tools;

public class Application {
    public String getFileFromResource(String name){
        return getClass().getClassLoader().getResource(name).getFile();
    }

    public static void printString(String s){
        System.out.println(s);
    }
    public static void printInt(Integer i){ System.out.println(i); }
}
