package com.exercise.henry.convert;

import org.apache.parquet.example.data.Group;

public interface ValueConverter {

    Group convertGroup(Group group);

}
