package com.kedacom.flink;
import groovy.util.logging.Slf4j;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RunWith(JUnit4.class)
public class ScriptEngineTests {
    ScriptEngineManager factory = new ScriptEngineManager();
    ScriptEngine engine = factory.getEngineByName("groovy");
    @Test
    public void loadScript() throws Exception{
        test("sql/udf.groovy");
//        test("sql/udf2.groovy");
    }

    @Test
    public void test(){
        String value="192.168.7.123.taskmanager.f825bae3c3355b475f1a3e9859e65843.insert-into_default_catalog.default_database.device_print.Sink: Sink(table\\u003d[default_catalog.default_database.device_print], fields\\u003d[gbid, .0.numRecordsIn";
        Pattern p=Pattern.compile("^([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)\\.([a-z]+)\\.([0-9a-zA-Z]+)\\.(.+)\\.([0-9a-zA-Z]+\\.[a-zA-Z]+)$");
        Matcher matcher=p.matcher(value);
        if (matcher.matches()){
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
            System.out.println(matcher.group(5));
//            System.out.println(matcher.group(6));
        }
    }

    public void test(String path) throws Exception{
        File file=new File(path);
        SimpleBindings bindings=new SimpleBindings();
        ArrayList<Class> list=new ArrayList<>();
        bindings.put("list",list);
        try(FileInputStream fileInputStream=new FileInputStream(file)){
            engine.eval(IOUtils.toString(fileInputStream,Charsets.UTF_8.name()),bindings);
        }

//        ScalarFunction scalarFunction =(ScalarFunction)list.get(0).getClassLoader().loadClass("test.Udf").newInstance();
//        System.out.println(list.get(0).getClassLoader());
    }
}
