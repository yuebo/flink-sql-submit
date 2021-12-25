package com.eappcat.flink.parser;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.eappcat.flink.parser.SqlCommandParser.SINGLE_OPERAND;

/**
 * Supported SQL commands.
 */
public enum SqlCommand {
    /*插入操作*/
    INSERT_INTO("(INSERT\\s+INTO.*)", SINGLE_OPERAND),
    /*创建表*/
    CREATE_TABLE("(CREATE\\s+TABLE.*)", SINGLE_OPERAND),
    /*创建函数*/
    CREATE_FUNCTION("(CREATE\\s+FUNCTION.*)", SINGLE_OPERAND),
    CREATE_INLINE_CLASS("(CREATE\\s+INLINE\\s+CLASS\\s+([0-9a-zA-Z\\\\.])+\\s+AS\\s+(.+)END$)", (operands) -> {
        if (operands.length < 3) {
            return Optional.empty();
        } else if (operands[0] == null) {
            return Optional.of(new String[0]);
        }
        return Optional.of(new String[]{operands[1], operands[2]});
    }),
    /*删除函数*/
    DROP_FUNCTION("(DROP\\s+FUNCTION.*)", SINGLE_OPERAND),
    /*删除表*/
    DROP_TABLE("(DROP\\s+TABLE.*)", SINGLE_OPERAND),
    /*创建视图*/
    CREATE_VIEW("(CREATE\\s+VIEW.*)", SINGLE_OPERAND),
    DROP_VIEW("(DROP\\s+VIEW.*)", SINGLE_OPERAND),
    SELECT("(SELECT\\s+.*)", SINGLE_OPERAND),
    /*设置参数*/
    SET("SET(\\s+(\\S+)\\s*=(.*))?", (operands) -> {
        if (operands.length < 3) {
            return Optional.empty();
        } else if (operands[0] == null) {
            return Optional.of(new String[0]);
        }
        return Optional.of(new String[]{operands[1], operands[2]});
    });


    private static final int DEFAULT_PATTERN_FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
    public final Pattern pattern;
    public final Function<String[], Optional<String[]>> operandConverter;

    SqlCommand(String matchingRegex, Function<String[], Optional<String[]>> operandConverter) {
        this.pattern = Pattern.compile(matchingRegex, DEFAULT_PATTERN_FLAGS);
        this.operandConverter = operandConverter;
    }

    @Override
    public String toString() {
        return super.toString().replace('_', ' ');
    }
}