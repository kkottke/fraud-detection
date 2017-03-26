package de.kkottke.fraud_detection.examples.util;

import java.io.FileWriter;
import java.io.IOException;

public class DataGenerator {

    public static void main(String[] args) throws IOException {
        FileWriter writer = new FileWriter("/Users/kkt/Projekte/Flink/input/testdata.csv");

        for (int i = 0; i < 200000; i++) {
            writer.write("user" + i + ":user,0162-" + i + ":handy\n");
            writer.write("user" + i + ":user,street-" + i + ":post\n");
            writer.write("user" + i + ":user,user-" + i + "@mail.com:mail\n");
            writer.write("user" + i + ":user,DE-" + i + ":bank\n");
            writer.write("user" + i + ":user," + i + ":card\n");
        }

        writer.flush();
        writer.close();
    }
}
