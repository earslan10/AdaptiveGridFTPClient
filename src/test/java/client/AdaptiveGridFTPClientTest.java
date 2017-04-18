package client;

import client.AdaptiveGridFTPClient.TransferAlgorithm;
import client.hysterisis.Entry;
import client.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.module.CooperativeModule.StorkFTPClient;
import stork.util.XferList;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdaptiveGridFTPClientTest {
  private final String SRC_ADDR = "gsiftp://gridftp.stampede.tacc.xsede.org:2811/scratch/01814/earslan/mixedDataset/";
  private final String DST_ADDR = "gsiftp://gsiftp://oasis-dm.sdsc.xsede.org:2811/oasis/scratch/earslan/temp_project/mixedDataset/";
  //@Mock private Hysterisis mockHysterisis;
  private final String SRC_DIR = "/scratch/01814/earslan/mixedDataset/";
  private final String DST_DIR = "/oasis/scratch/earslan/temp_project/mixedDataset/";
  private final String sampleInputFile = "/sampleFileList";
  @Mock
  private GridFTPTransfer mockGridFTPClient;
  @Mock
  private StorkFTPClient mockStorkFTPClient;
  private AdaptiveGridFTPClient multiChunk;
  private XferList files;


  private final Entry buildIntendedTransfer() {
    Entry e = new Entry();
    e.setSource(SRC_ADDR);
    e.setDestination(DST_ADDR);
    e.setBandwidth(Math.pow(10, 10));
    e.setRtt(0.04);
    e.setBDP((0.04 * Math.pow(10, 10)) / 8);
    e.setConcurrency(10);
    e.setBufferSize(1024 * 1024 * 32);
    return e;
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    multiChunk = new AdaptiveGridFTPClient(mockGridFTPClient);
    AdaptiveGridFTPClient.transferTask = buildIntendedTransfer();

    files = Utils.readInputFilesFromFile(AdaptiveGridFTPClientTest.class.getResourceAsStream(sampleInputFile),
            SRC_DIR, DST_DIR);
    when(mockGridFTPClient.getListofFiles(any(String.class), any(String.class)))
            .thenReturn(files);
    when(mockGridFTPClient.runTransfer(any(Partition.class)))
            .thenReturn(611.8585207)
            .thenReturn(740.0)
            .thenReturn(1437.0)
            .thenReturn(612.0);

  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testHysterisis() throws Exception {
    multiChunk.setUseHysterisis(true);
    multiChunk.algorithm = TransferAlgorithm.MULTICHUNK;
    multiChunk.transfer();
  }

  @Test
  public void testPartitionByFileSize() throws Exception {
    multiChunk = new AdaptiveGridFTPClient(mockGridFTPClient);
    Entry mockTransferTask = mock(Entry.class);
    AdaptiveGridFTPClient.transferTask = mockTransferTask;
    when(mockTransferTask.getBandwidth()).thenReturn(320.0);
    XferList xl = new XferList("/dir1/dir2/", "/dir3/dir4");
    xl.add("/dir1/dir2/fileA", 100);
    xl.add("/dir1/dir2/fileB", 200);
    xl.add("/dir1/dir2/fileC", 300);
    System.out.println("Count  " + xl.count());
    List<Partition> chunks = multiChunk.partitionByFileSize(xl, 4);
    System.out.println("Count  " + xl.count());
    assertEquals(chunks.size(), 1);
    assertEquals(chunks.get(0).getDensity(), Utils.Density.HUGE);

    for (int i = 0; i < 10000; i++) {
      xl.add("/dir1/dir2/" + String.valueOf(i), 1);
      //System.out.println("Count  " + xl.count());
    }
    for (int i = 0; i < 1000; i++) {
      xl.add("/dir1/dir2/" + String.valueOf(i), 5);
    }
    for (int i = 0; i < 100; i++) {
      xl.add("/dir1/dir2/" + String.valueOf(i), 60);
    }

    chunks = multiChunk.partitionByFileSize(xl, 4);
    //Collections.sort(chunks);
    assertEquals(chunks.size(), 4);
    assertEquals(chunks.get(0).getRecords().count(), 10000);
    assertEquals(chunks.get(1).getRecords().count(), 1000);
    assertEquals(chunks.get(2).getRecords().count(), 100);
    assertEquals(chunks.get(3).getRecords().count(), 3);

    chunks = multiChunk.partitionByFileSize(xl, 3);
    assertEquals(chunks.size(), 3);
    assertEquals(chunks.get(0).getRecords().count(), 10000);
    assertEquals(chunks.get(1).getRecords().count(), 1000);
    assertEquals(chunks.get(2).getRecords().count(), 103);

    chunks = multiChunk.partitionByFileSize(xl, 2);
    assertEquals(chunks.size(), 2);
    assertEquals(chunks.get(0).getRecords().count(), 10000);
    assertEquals(chunks.get(1).getRecords().count(), 1103);
  }

}
