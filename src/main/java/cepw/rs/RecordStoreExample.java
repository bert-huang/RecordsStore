package cepw.rs;

import cepw.rs.store.RecordReader;
import cepw.rs.store.RecordWriter;
import cepw.rs.store.MemIndexedRecordsStore;
import java.io.File;
import java.util.Date;

public class RecordStoreExample {

  public static void main(String[] args) throws Exception {

    /* Initialise the records store */
    System.out.println("Initialising store");
    File store = new File("testDatabase.jsrs");
    MemIndexedRecordsStore memIndexedRecordsStore;

    if (store.exists()) {
      System.out.println("Using existing store: " + store.getPath());
      memIndexedRecordsStore = new MemIndexedRecordsStore(store.getPath(), "rw");
    } else {
      System.out.println("Creating new store: " + store.getPath());
      memIndexedRecordsStore = new MemIndexedRecordsStore(store.getPath(), 256);
    }

    RecordWriter createRw;
    RecordWriter updateRw;
    RecordReader readRr;

    /* Create */
    System.out.println("Creating new record with key: " + "foo.lastAccessTime");
    createRw = new RecordWriter("foo.lastAccessTime");
    createRw.writeObject(new Date(12345));
    memIndexedRecordsStore.insert(createRw);

    /* Read */
    System.out.println("Reading record with key: " + "foo.lastAccessTime");
    readRr = memIndexedRecordsStore.read("foo.lastAccessTime");
    Object object1 = readRr.readObject();
    System.out.println("last access was at: " + object1);

    /* Update */
    System.out.println("Updating record with key: " + "foo.lastAccessTime");
    updateRw = new RecordWriter("foo.lastAccessTime");
    updateRw.writeObject(new Date());
    memIndexedRecordsStore.update(updateRw);

    /* Read */
    System.out.println("Reading record with key: " + "foo.lastAccessTime");
    readRr = memIndexedRecordsStore.read("foo.lastAccessTime");
    Object object2 = readRr.readObject();
    System.out.println("last access was at: " + object2);

        /* Create */
    System.out.println("Creating new record with key: " + "foo.description");
    createRw = new RecordWriter("foo.description");
    createRw.writeObject("this is a test description");
    memIndexedRecordsStore.insert(createRw);

    /* Iterate */
    System.out.println("Reading all records...");
    int index = 1;
    for (String s : memIndexedRecordsStore.keys()) {
      System.out.println(index + ": " + memIndexedRecordsStore.read(s).readObject());
      index++;
    }

    /* Delete */
    System.out.println("Deleting record with key: " + "foo.description");
    memIndexedRecordsStore.delete("foo.description");

    /* Delete */
    System.out.println("Deleting record with key: " + "foo.lastAccessTime");
    memIndexedRecordsStore.delete("foo.lastAccessTime");

    /* Close */
    memIndexedRecordsStore.close();
  }
}
