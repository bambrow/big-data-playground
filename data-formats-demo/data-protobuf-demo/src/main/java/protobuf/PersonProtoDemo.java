package protobuf;

import protobuf.PersonProto.Person;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PersonProtoDemo {
    public static void main(String[] args) {
        Person p1 = Person.newBuilder()
                .setName("David")
                .setEmail("david@a.com")
                .setId(1)
                .addPhone(Person.PhoneNumber.newBuilder()
                        .setNumber("12345")
                        .setType(0))
                .addPhone(Person.PhoneNumber.newBuilder()
                        .setNumber("67890")
                        .setType(1))
                .build();

        String output = "data-formats-demo/data-protobuf-demo/src/main/resources/protobuf/person.txt";
        Path path = Paths.get(output);
        try {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            FileOutputStream out = new FileOutputStream(output);
            p1.writeTo(out);
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            FileInputStream in = new FileInputStream(output);
            Person p2 = Person.parseFrom(in);
            System.out.println(p2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
