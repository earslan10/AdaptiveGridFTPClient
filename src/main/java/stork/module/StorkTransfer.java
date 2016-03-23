package stork.module;

interface StorkTransfer extends Runnable {
  void start();

  void stop();

  int waitFor();
}
