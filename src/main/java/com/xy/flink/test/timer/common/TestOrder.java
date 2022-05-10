package com.xy.flink.test.timer.common;

public class TestOrder {
    Long id;
    String order_no;
    Integer order_status;
    Long ctime;
    Long mtime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getOrder_no() {
        return order_no;
    }

    public void setOrder_no(String order_no) {
        this.order_no = order_no;
    }

    public Integer getOrder_status() {
        return order_status;
    }

    public void setOrder_status(Integer order_status) {
        this.order_status = order_status;
    }

    public Long getCtime() {
        return ctime;
    }

    public void setCtime(Long ctime) {
        this.ctime = ctime;
    }

    public Long getMtime() {
        return mtime;
    }

    public void setMtime(Long mtime) {
        this.mtime = mtime;
    }

    @Override
    public String toString() {
        return "TestOrder{" +
                "id=" + id +
                ", order_no='" + order_no + '\'' +
                ", order_status=" + order_status +
                ", ctime=" + ctime +
                ", mtime=" + mtime +
                '}';
    }
}
