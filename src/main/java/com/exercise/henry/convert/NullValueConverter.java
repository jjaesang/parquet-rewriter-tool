package com.exercise.henry.convert;

import com.exercise.henry.option.ParquetRewriterJobOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.List;

@Slf4j
public class NullValueConverter implements ValueConverter {

    private String keyColName;
    private List<String> keyColValueList;
    private List<String> targetColNameList;
    private MessageType schema;
    private SimpleGroupFactory rowGroupFactory;

    public NullValueConverter(ParquetRewriterJobOption parquetRewriterJobOption) {
        this.keyColName = parquetRewriterJobOption.getKeyColName();
        this.keyColValueList = parquetRewriterJobOption.getKeyColValueList();
        this.targetColNameList = parquetRewriterJobOption.getTargetColNameList();
        this.schema = parquetRewriterJobOption.getSchema();
        this.rowGroupFactory = new SimpleGroupFactory(schema);
    }

    @Override
    public Group convertGroup(Group group) {
        if (isNotConvertRow(group)) {
            return group;
        }


        Group targetGroup = rowGroupFactory.newGroup();
        int fieldCount = schema.getFieldCount();
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            if (isConvertNullColumn(fieldIndex)) {
                log.info("convert to null value -> {} ", schema.getFieldName(fieldIndex));
                continue;
            }
            convertColumnValue(targetGroup, fieldIndex, group);
        }

        log.info("[ Before] group is converted Row -> \n{}", group.toString());
        log.info("[ After ] group is converted Row -> \n{}", targetGroup.toString());
        log.info("[ STAT  ] compare field count    -> {} {}", group.getType().getFieldCount(), targetGroup.getType().getFieldCount());

        return targetGroup;
    }


    private boolean isConvertRow(Group group) {
        int keyColIndex = schema.getFieldIndex(keyColName);
        return keyColValueList.contains(group.getValueToString(keyColIndex, 0));
    }

    private boolean isNotConvertRow(Group group) {
        return !isConvertRow(group);
    }

    private boolean isConvertNullColumn(int fieldIndex) {
        return targetColNameList.contains(schema.getFieldName(fieldIndex));
    }

    private boolean isNotConvertNullColumn(int fieldIndex) {
        return !isConvertNullColumn(fieldIndex);
    }

    private void convertColumnValue(Group targetGroup, int index, Group sourceGroup) {
        GroupType type = sourceGroup.getType();
        PrimitiveType.PrimitiveTypeName columnType = type.getType(index).asPrimitiveType()
                .getPrimitiveTypeName();
        String fieldName = type.getFieldName(index);

        int fieldRepetitionCount = sourceGroup.getFieldRepetitionCount(index);
        for (int fieldIndex = 0; fieldIndex < fieldRepetitionCount; fieldIndex++) {
            switch (columnType) {
                case BINARY:
                    targetGroup.add(fieldName, sourceGroup.getBinary(schema.getFieldName(index), fieldIndex));
                    break;
                case BOOLEAN:
                    targetGroup.add(fieldName, sourceGroup.getBoolean(schema.getFieldName(index), fieldIndex));
                    break;
                case DOUBLE:
                    targetGroup.add(fieldName, sourceGroup.getDouble(schema.getFieldName(index), fieldIndex));
                    break;
                case FLOAT:
                    targetGroup.add(fieldName, sourceGroup.getFloat(schema.getFieldName(index), fieldIndex));
                    break;
                case INT32:
                    targetGroup.add(fieldName, sourceGroup.getInteger(schema.getFieldName(index), fieldIndex));
                    break;
                case INT64:
                    targetGroup.add(fieldName, sourceGroup.getLong(schema.getFieldName(index), fieldIndex));
                    break;
                case INT96:
                    targetGroup.add(fieldName, sourceGroup.getInt96(schema.getFieldName(index), fieldIndex));
                    break;
                default:
                    throw new IllegalArgumentException("Not Support Type!");
            }
        }
    }


}
