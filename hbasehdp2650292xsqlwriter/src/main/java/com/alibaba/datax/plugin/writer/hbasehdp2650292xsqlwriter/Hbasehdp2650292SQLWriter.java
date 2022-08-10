package com.alibaba.datax.plugin.writer.hbasehdp2650292xsqlwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yanghan.y
 */
public class Hbasehdp2650292SQLWriter extends Writer {
    public static class Job extends Writer.Job {
        private Hbasehdp2650292SQLWriterConfig config;

        @Override
        public void init() {
            // 解析配置
            config = Hbasehdp2650292SQLHelper.parseConfig(this.getPluginJobConf());

            // 校验配置，会访问集群来检查表
            Hbasehdp2650292SQLHelper.validateConfig(config);
        }

        @Override
        public void  prepare() {
            // 写之前是否要清空目标表，默认不清空
            if(config.truncate()) {
                Connection conn = Hbasehdp2650292SQLHelper.getJdbcConnection(config);
                Hbasehdp2650292SQLHelper.truncateTable(conn, config.getTableName());
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int j = 0; j < mandatoryNumber; j++) {
                splitResultConfigs.add(config.getOriginalConfig().clone());
            }
            return splitResultConfigs;
        }

        @Override
        public void destroy() {
            // NOOP
        }
    }

    public static class Task extends Writer.Task {
        private Configuration taskConfig;
        private Hbasehdp2650292SQLWriterTask hbasehdp2650292SQLWriterTask;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.hbasehdp2650292SQLWriterTask = new Hbasehdp2650292SQLWriterTask(this.taskConfig);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            this.hbasehdp2650292SQLWriterTask.startWriter(lineReceiver, super.getTaskPluginCollector());
        }


        @Override
        public void destroy() {
            // hbaseSQLTask不需要close
        }
    }
}
