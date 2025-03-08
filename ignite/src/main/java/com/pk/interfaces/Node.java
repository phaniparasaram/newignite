package com.pk.interfaces;

public interface Node {
    public void start();
    public boolean isLeader();
    public void stop();
    public void addData(String key, String data);
    public String getData(String key);
}
