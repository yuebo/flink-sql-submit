package com.eappcat.flink.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.tools.GroovyClass;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

/**
 * GroovyLoader
 * 在集群环境下, groovy的jar包应该是通过参数指定的
 */
public class GroovyUtils {
    private static volatile boolean loaded=false;
    public static void hackClassPath() throws Exception {

		URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		// 反射获取类加载器中的addURL方法，并将需要加载类的jar路径
		Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
		if (!method.isAccessible()) {
			method.setAccessible(true);
		}
		File rootDir=FileUtils.jobTempDir();
		URL url = rootDir.toURI().toURL();
		// 把当前jar的路径加入到类加载器需要扫描的路径
		method.invoke(classLoader, url);
	}
    public static void compile(StreamTableEnvironment environment,String key, String function) throws Exception{
        if(!"true".equals(System.getProperty("flink.local.mock"))){
            return;
        }
        if (!loaded){
            hackClassPath();
        }
        CompilationUnit cu = new CompilationUnit();
        cu.addSource(key,function);
        cu.compile();
        List<GroovyClass> groovyClasses=cu.getClasses();
        for (GroovyClass groovyClass:groovyClasses) {
            String value=StringUtils.replace(groovyClass.getName(),".","/").concat(".class");
            File file=new File(FileUtils.jobTempDir(),value);
            file.getParentFile().mkdirs();
            try(FileOutputStream fileOutputStream=new FileOutputStream(file)) {
                IOUtils.write(groovyClass.getBytes(),fileOutputStream);
            }
            Class clazz=Class.forName(groovyClass.getName());
            if(UserDefinedFunction.class.isAssignableFrom(clazz)){
                //注册函数
                environment.createTemporaryFunction(key,clazz);
            }
        }
    }
}
