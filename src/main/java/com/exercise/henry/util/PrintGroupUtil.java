package com.exercise.henry.util;


import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

public class PrintGroupUtil {

    public static void printGroup(Group group) {
        int fieldCount = group.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = group.getFieldRepetitionCount(field);
            Type fieldType = group.getType().getType(field);
            String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(fieldName + "\n\t" + group.getValueToString(field, index) + " (" + group.getType().getType(field) + ") ");
                }
            }
        }
    }

    public static void printFieldCount(Group group) {
        System.out.println(group.getType().getFieldCount());

    }
}
