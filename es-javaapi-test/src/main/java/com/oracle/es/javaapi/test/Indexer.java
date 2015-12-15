package com.oracle.es.javaapi.test;

public interface Indexer<T> {

    boolean createIndex();

    boolean upgradeIndex();

    boolean indexDocument(T document);

    boolean deleteDocument(T document);

}
