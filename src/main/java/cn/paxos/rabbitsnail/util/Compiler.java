package cn.paxos.rabbitsnail.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Compiler {
	
	private static final String TMP = "/tmp/";
	private static final String CLASSPATH;
	private static final ClassLoader CLASSLOADER;
	
	static {
		URLClassLoader ucl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
		StringBuilder classpathSB = new StringBuilder(new File(TMP).getAbsolutePath());
		String seperator = System.getProperty("os.name").toLowerCase().indexOf("win") > -1 ? ";" : ":";
		for (URL url : ucl.getURLs()) {
			classpathSB.append(seperator);
			classpathSB.append(url.getFile().substring(1));
		}
		CLASSPATH = classpathSB.toString();
		try {
			CLASSLOADER = new URLClassLoader(new URL[]{new File(TMP).toURI().toURL()});
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException, IOException, InterruptedException, ClassNotFoundException {
		Class<?> cls = Compiler.compile("X", "public class X extends cn.paxos.rabbitsnail.util.Compiler {}");
		System.out.println((Compiler) cls.newInstance());
	}

	public static Class<?> compile(String className, String java) {
		try {
			File javaFile = new File(TMP + className + ".java");
			javaFile.deleteOnExit();
			FileOutputStream out = new FileOutputStream(javaFile);
			out.write(java.getBytes("UTF-8"));
			out.close();
			Runtime rt = Runtime.getRuntime();
			Process p = rt.exec("javac -classpath " + CLASSPATH + " " + javaFile.getAbsolutePath());
			InputStream is = p.getInputStream();
			InputStream es = p.getErrorStream();
			byte[] buff = new byte[4096];
			while (is.read(buff) > 0) {}
			while (es.read(buff) > 0) {}
			p.waitFor();
			File classFile = new File(TMP + className + ".class");
			classFile.deleteOnExit();
			Class<?> type = CLASSLOADER.loadClass(className);
			return type;
		} catch (Exception e) {
			throw new RuntimeException("Failed to hack type " + className + ": " + java, e);
		}
	}

}
