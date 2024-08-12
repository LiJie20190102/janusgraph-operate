package com.qsdi.bigdata.graph.gstore.performance.test.job.entity.enums;

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Description
 *
 * @author lijie0203 2024/7/16 19:19
 */
public enum TaskType {
    kafka2graph(1),kafka2kafka(2);

    private int code;

    TaskType(int code) {
        this.code = code;
    }

    private static final Map<Integer, TaskType> CODE_2_TYPE = Arrays.stream(TaskType.values()).collect(Collectors.toMap(x -> x.code, x -> x));

    public static TaskType getByCode(int code) {
            Preconditions.checkArgument(CODE_2_TYPE.containsKey(code),String.format("code:%s is not exist",code));
        return CODE_2_TYPE.get(code);
    }
}
