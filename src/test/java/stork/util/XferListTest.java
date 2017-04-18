package stork.util;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by earslan on 4/17/17.
 */
public class XferListTest {
  @Before
  public void setUp() throws Exception {

  }

  @Test
  public void testConstructor() {
    XferList xl = new XferList("/dir1/dir2/testFile", "/destDir/testFile", 100);
    assertEquals(xl.count(), 1);
    assertEquals(xl.getItem(0).fileName, "testFile");
    assertEquals(xl.getItem(0).spath, "/dir1/dir2/testFile");
    xl.updateDestinationPaths();
    assertEquals(xl.getItem(0).dpath, "/destDir/testFile");
  }

  @Test
  public void testAdd() {
    XferList xl = new XferList("/dir1/dir2/", "/dir3/dir4");
    xl.add("/dir1/dir2/fileA", 100);
    xl.add("/dir1/dir2/fileB", 200);
    assertEquals(xl.count(), 2);
    assertEquals(xl.getItem(0).spath, "/dir1/dir2/fileA");
    assertEquals(xl.getItem(1).spath, "/dir1/dir2/fileB");
    xl.updateDestinationPaths();
    assertEquals(xl.getItem(0).dpath, "/dir3/dir4/fileA");
  }

}