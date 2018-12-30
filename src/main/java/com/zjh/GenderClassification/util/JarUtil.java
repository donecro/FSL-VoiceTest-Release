package com.zjh.GenderClassification.util;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
public class JarUtil {
    public static String jar(Class<?> cls){// ��֤ok
        String outputJar =cls.getName()+".jar";
        String input = cls.getClassLoader().getResource("").getFile();
        input= input.substring(0,input.length()-1);
        input = input.substring(0,input.lastIndexOf("/")+1);
        input =input +"bin/";
        jar(input,outputJar);
        return outputJar;
    }
    private static void jar(String inputFileName, String outputFileName){
        JarOutputStream out = null;
        try{
            out = new JarOutputStream(new FileOutputStream(outputFileName));
            File f = new File(inputFileName);
            jar(out, f, "");
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
    private static void jar(JarOutputStream out, File f, String base) throws Exception {
        if (f.isDirectory()) {
            File[] fl = f.listFiles();
            base = base.length() == 0 ? "" : base + "/"; // ע�⣬��������б��
            for (int i = 0; i < fl.length; i++) {
                jar(out, fl[ i], base + fl[ i].getName());
            }
        } else {
            out.putNextEntry(new JarEntry(base));
            FileInputStream in = new FileInputStream(f);
            byte[] buffer = new byte[1024];
            int n = in.read(buffer);
            while (n != -1) {
                out.write(buffer, 0, n);
                n = in.read(buffer);
            }
            in.close();
        }
    }
}
