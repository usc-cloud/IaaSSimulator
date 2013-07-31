package edu.usc.pgroup.cloud.floesimulator.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AppConfig {
	Properties p;
	
	private AppConfig()
	{
		p = new Properties();
		try {
			p.load(new FileInputStream("appgraph\\config.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static AppConfig instance = null;
	public static synchronized AppConfig getInstance()
	{
		if(instance == null)
		{
			instance = new AppConfig();
		}
		return instance;
	}
	
	public String get(String key) {
		return p.getProperty(key);
	}
	public void set(String key, String value) {
		p.setProperty(key, value);
	}
}
