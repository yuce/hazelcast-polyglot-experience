package org.example;

public class Person {
    public static final int CLASS_ID = 1;

    private String name;
    private int age;

    public Person() {
        this(null, 0);
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return String.format("Person(%s, %d)", getName(), getAge());
    }

    /*
    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }
*/
}
