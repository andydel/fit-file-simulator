package org.agd;

import net.studioblueplanet.fitreader.FitDataRecord;
import net.studioblueplanet.fitreader.FitMessageField;
import net.studioblueplanet.fitreader.FitReader;
import org.agd.entity.FieldDefinition;
import org.agd.fitfile.avro.FitSample;

import java.util.*;
import java.time.Instant;
import java.util.Map;

public class FitFileRunner {
    public static void main(String[] args) {

        var reader = FitReader.getInstance();
        var fitFile = reader.readFile("/Users/andy/IdeaProjects/fit-file-simulator/src/main/resources/Karoo-Afternoon_Ride-2024-05-04-1522.fit");
        var kafkaPublisher = new KafkaPublisher();

        System.out.println("== Message definitions ==");
        fitFile.getMessageNames().forEach(name -> System.out.println(name));


        fitFile.getAllMessages("field_description").forEach(message -> {
            System.out.println("== Field Description ==");
            message.getDataRecords().forEach(record -> {
                var fieldDefinitionNumber = record.bytesToUnsignedInt(0, 1);
                var size = record.bytesToUnsignedInt(1, 1);
                var baseType = record.bytesToUnsignedInt(2, 1);
                System.out.println(String.format("Field Definition Number: %d, Size: %d, Base Type: %d", fieldDefinitionNumber, size, baseType));
            });
        });

        fitFile.getAllMessages("record").forEach(message -> {
            System.out.println("== Record == " + message.getNumberOfRecords());

            final Map<String, FieldDefinition> fieldMap = new HashMap<>();
            message.getFieldDefintions().forEach(fm -> {
                fieldMap.put(fm.definition.fieldName, new FieldDefinition(fm.definition.fieldName, fm.size, fm.byteArrayPosition, fm.definition.scale, fm.definition.offset));
            });

            message.getDataRecords().forEach(record -> {
                var timestamp = record.bytesToUnsignedLong(0, 4) * 1000;

                var fitSample = new FitSample();
                fitSample.setTimestamp(Instant.ofEpochMilli(timestamp));
                if (fieldMap.containsKey("grade")) {
                    var fd = fieldMap.get("grade");
                    var grade = getLong(record, fd, true);
                    fitSample.setGrade((double)grade / 11930465.0); //conver t o degrees
                }
                if (fieldMap.containsKey("speed")) {
                    var fd =  fieldMap.get("speed");
                    var speed = getLong(record, fd, false);
                    //Speed in mm per second, convert to kph
                    var convertedSpeed = ((double)speed * 3600) / 1000000;
                    fitSample.setSpeed(convertedSpeed);
                }
                if (fieldMap.containsKey("heart_rate")) {
                    fitSample.setHeartRate(getLong(record, fieldMap.get("heart_rate"), false));
                }
                if (fieldMap.containsKey("cadence")) {
                    fitSample.setCadence(getLong(record, fieldMap.get("cadence"), false));
                }
                if (fieldMap.containsKey("power")) {
                    fitSample.setPower(getLong(record, fieldMap.get("power"), false));
                }
                if (fieldMap.containsKey("distance")) {
                    var fd = fieldMap.get("distance");
                    var ditanceInVM = getLong(record, fd, false);
                    var convertedDistance = ((double)ditanceInVM / 100.0) / 1000.0;
                    fitSample.setDistance(convertedDistance);
                }
                if (fieldMap.containsKey("altitude")) {
                    var fd = fieldMap.get("altitude");
                    var altitude = getLong(record, fd, true);
                    var convertedAltitude = ((double)altitude / fd.scale()) - fd.offset();
                    fitSample.setAltitude(convertedAltitude);
                }
                if (fieldMap.containsKey("position_lat")) {
                    fitSample.setPositionLat((double)getLong(record, fieldMap.get("position_lat"), true) / 11930465.0);
                }
                if (fieldMap.containsKey("position_long")) {
                    fitSample.setPositionLong((double)getLong(record, fieldMap.get("position_long"), true) / 11930465.0);
                }

                kafkaPublisher.publishMessage(fitSample);
            });

        });
        new Thread(kafkaPublisher).start();

    }

    private static long getLong(FitDataRecord record, FieldDefinition fd, boolean signed) {
        if (signed) {
            return record.bytesToSignedLong(fd.byteArrayPosition(), fd.size());
        } else {
            return record.bytesToUnsignedLong(fd.byteArrayPosition(), fd.size());
        }
    }

}
