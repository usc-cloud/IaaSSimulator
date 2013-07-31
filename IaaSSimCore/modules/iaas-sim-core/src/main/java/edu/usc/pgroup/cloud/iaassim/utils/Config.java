package edu.usc.pgroup.cloud.iaassim.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {
	Properties p;
	
	private Config()
	{
		p = new Properties();
		try {
			p.load(new FileInputStream("conf\\IaaSSim.conf"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static Config instance = null;
	public static synchronized Config getInstance()
	{
		if(instance == null)
		{
			instance = new Config();
		}
		return instance;
	}
	
	public String get(String key) {
		return p.getProperty(key);
	}
}
