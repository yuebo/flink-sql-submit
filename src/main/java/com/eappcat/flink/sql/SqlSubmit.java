package com.eappcat.flink.sql;

import com.eappcat.flink.http.RequestUtils;
import com.eappcat.flink.parser.SqlCommandParser;
import com.eappcat.flink.utils.GroovyUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *  SQL提交函数
 */
public class SqlSubmit {
    private static final Logger logger=LoggerFactory.getLogger(SqlSubmit.class.getName());
    private UserOptions userOptions;
    private StreamTableEnvironment tEnv;
    private ScriptEngineManager factory = new ScriptEngineManager();
    private ScriptEngine engine = factory.getEngineByName("groovy");

    public SqlSubmit(UserOptions options, StreamTableEnvironment tEnv) {
        this.userOptions = options;
        this.tEnv = tEnv;
    }

    public void run() throws Exception {
        //从指定位置读取sql
        List<String> sql = new ArrayList<>();
        if (Source.FILE.equals(userOptions.getSource())||userOptions.getSource() == null){
            sql.addAll(Files.readAllLines(Paths.get(userOptions.getSqlFilePath())));
        }else if(Source.URL.equals(userOptions.getSource())){
            String[] result= RequestUtils.get(userOptions.getSqlFilePath()).split("\n");
            sql.addAll(Arrays.asList(result));
        }
        //解析sql
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
    }

    private void callCommand(SqlCommandParser.SqlCommandCall cmdCall) throws Exception{
        logger.info("加载SQL: \n {}",cmdCall.operands[0]);

        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
                callCreateTable(cmdCall);
                break;
            case INSERT_INTO:
                callInsertInto(cmdCall);
                break;
            case CREATE_VIEW:
                callCreateView(cmdCall);
                break;
            case CREATE_FUNCTION:
                callCreateFunction(cmdCall);
                break;
            case DROP_FUNCTION:
                callDropFunction(cmdCall);
                break;
            case CREATE_INLINE_CLASS:
                callCreateInlineClass(cmdCall);
                break;
            case DROP_TABLE:
                callDropTable(cmdCall);
                break;
            case DROP_VIEW:
                callDropView(cmdCall);
                break;
            case SELECT:
                callSelectQuery(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callCreateInlineClass(SqlCommandParser.SqlCommandCall cmdCall)throws Exception {
        String key = cmdCall.operands[0];
        String function = cmdCall.operands[1];
        //动态编译groovy成Class
        logger.info("加载代码{}",function);

        GroovyUtils.compile(this.tEnv,key,function);

    }

    private void callDropView(SqlCommandParser.SqlCommandCall cmdCall) {
        this.executeDDL(cmdCall);
    }

    private void callSelectQuery(SqlCommandParser.SqlCommandCall cmdCall) {
        String sql=cmdCall.operands[0];
        try {
            tEnv.sqlQuery(sql);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + sql + "\n", e);
        }

    }

    private void executeDDL(SqlCommandParser.SqlCommandCall cmdCall){
        String ddl=cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }
    private void callCreateFunction(SqlCommandParser.SqlCommandCall cmdCall){
        this.executeDDL(cmdCall);
    }


    private void callDropTable(SqlCommandParser.SqlCommandCall cmdCall){
        this.executeDDL(cmdCall);
    }

    private void callDropFunction(SqlCommandParser.SqlCommandCall cmdCall){
        this.executeDDL(cmdCall);
    }
    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];
        tEnv.getConfig().getConfiguration().setString(key, StringUtils.trim(value));
    }

    private void callCreateTable(SqlCommandParser.SqlCommandCall cmdCall) {
        this.executeDDL(cmdCall);
    }

    private void callInsertInto(SqlCommandParser.SqlCommandCall cmdCall) {
        this.executeDDL(cmdCall);
    }

    private static final Pattern PATTERN=Pattern.compile("(?<= as).*",Pattern.DOTALL|Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN1=Pattern.compile("(?<=view ).*?(?= as)",Pattern.CASE_INSENSITIVE);

    private void callCreateView(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        //Pattern pattern=new Pattern();
        Matcher matcher = PATTERN.matcher(dml);
        Matcher matcher1 = PATTERN1.matcher(dml);
        if (matcher.find()&matcher1.find()){
            String sqlquery = matcher.group(0);
            String viewName = matcher1.group(0);
            tEnv.createTemporaryView(viewName,tEnv.sqlQuery(sqlquery));
        }else {
            throw new RuntimeException("Unsupported command '" + dml + "'");
        }
    }
}