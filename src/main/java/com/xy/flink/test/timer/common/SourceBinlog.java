package com.xy.flink.test.timer.common;

public class SourceBinlog {
    public String op;
    public TestOrder before;
    public TestOrder after;
    public String db;
    public String tableName;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public TestOrder getBefore() {
        return before;
    }

    public void setBefore(TestOrder before) {
        this.before = before;
    }

    public TestOrder getAfter() {
        return after;
    }

    public void setAfter(TestOrder after) {
        this.after = after;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public SourceBinlog() {
    }

    public SourceBinlog(String op, TestOrder before, TestOrder after, String db, String tableName) {
        this.op = op;
        this.before = before;
        this.after = after;
        this.db = db;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "SourceBinlog{" +
                "op='" + op + '\'' +
                ", before=" + before +
                ", after=" + after +
                ", db='" + db + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
