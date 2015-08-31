package stork.module;

public interface StorkTransfer extends Runnable {
  public void start();
  public void stop();
  public int waitFor();
}
