package org.abhishaw.roadrate.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConfig {
	private static Configuration config = null;

	private HbaseConfig() {
	}

	public static Configuration getHbaseConfiguration() {
		if (config == null) {
			config = HBaseConfiguration.create();
		}
		return config;
	}
}
