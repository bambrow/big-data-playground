package avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class PersonAvroDemo {
    public static void main(String[] args) {
        PhoneNumber n1 = PhoneNumber.newBuilder()
                .setNumber("12345")
                .setType(0)
                .build();
        PhoneNumber n2 = PhoneNumber.newBuilder()
                .setNumber("67890")
                .setType(1)
                .build();
        List<PhoneNumber> pn = new ArrayList<>();
        pn.add(n1);
        pn.add(n2);
        Person p1 = Person.newBuilder()
                .setName("David")
                .setEmail("david@a.com")
                .setId(1)
                .setPhone(pn)
                .build();

        String output = "data-formats-demo/data-avro-demo/src/main/resources/avro/person.txt";
        Path path = Paths.get(output);
        try {
            if (Files.exists(path)) {
                Files.delete(path);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File file = new File(output);

        try {
            DatumWriter<Person> datumWriter = new SpecificDatumWriter<>(Person.class);
            DataFileWriter<Person> fileWriter = new DataFileWriter<>(datumWriter);
            fileWriter.create(p1.getSchema(), file);
            fileWriter.append(p1);
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            DatumReader<Person> datumReader = new SpecificDatumReader<>(Person.class);
            DataFileReader<Person> fileReader = new DataFileReader<>(file, datumReader);
            Person p2 = null;
            while (fileReader.hasNext()) {
                p2 = fileReader.next(p2);
                System.out.println(p2);
            }
            fileReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
