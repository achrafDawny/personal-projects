package demo;

import java.io.File;
import java.io.FilenameFilter;

public class MyFilterImpl {
    public static void main(String[] args) {
        File dir = new File("java-refresher/src/main/java/core");

        // Using anonymous inner class
        dir.list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith("java");
            }
        });

        // Using lambda expression
        dir.list((dirname, name) -> name.endsWith("java"));
    }
}
