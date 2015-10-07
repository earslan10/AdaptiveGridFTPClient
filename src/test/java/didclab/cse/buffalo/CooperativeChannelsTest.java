package didclab.cse.buffalo;

import static org.mockito.Mockito.when;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.CooperativeChannels.TransferAlgorithm;
import didclab.cse.buffalo.hysterisis.Entry;
import didclab.cse.buffalo.utils.Utils;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.module.CooperativeModule.StorkFTPClient;
import stork.util.XferList;

public class CooperativeChannelsTest {
	@Mock private GridFTPTransfer mockGridFTPClient;
	@Mock private StorkFTPClient mockStorkFTPClient;
	//@Mock private Hysterisis mockHysterisis;
	
	private final String SRC_ADDR = "gsiftp://gridftp.stampede.tacc.xsede.org:2811/scratch/01814/earslan/mixedDataset/";
	private final String DST_ADDR = "gsiftp://gsiftp://oasis-dm.sdsc.xsede.org:2811/oasis/scratch/earslan/temp_project/mixedDataset/";
	private final String SRC_DIR = "/scratch/01814/earslan/mixedDataset/";
	private final String DST_DIR = "/oasis/scratch/earslan/temp_project/mixedDataset/";
	private final String sampleInputFile = "/sampleFileList";
	private CooperativeChannels multiChunk;
	private XferList files;
	

	private final Entry buildIntendedTransfer () {
		Entry e = new Entry();
		e.setSource(SRC_ADDR);
		e.setDestination(DST_ADDR);
		e.setBandwidth(Math.pow(10, 10));
		e.setRtt(0.04);
		e.setBDP((0.04 * Math.pow(10, 10)) / 8);
		e.setConcurrency(10);
		e.setBufferSize(1024*1024*32);
		return e;
	}
	
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		ConfigurationParams.MATLAB_DIR = "/Applications/MATLAB_R2014b.app/bin/";
		ConfigurationParams.MATLAB_DIR = "/Applications/MATLAB_R2014b.app/bin/";
		multiChunk = new CooperativeChannels(mockGridFTPClient);
		CooperativeChannels.intendedTransfer = buildIntendedTransfer();
		files = Utils.readInputFilesFromFile(getClass().getResourceAsStream(sampleInputFile),
				SRC_DIR, DST_DIR);
		when(mockGridFTPClient.getListofFiles(any(String.class), any(String.class)))
		.thenReturn(files);
		when(mockGridFTPClient.runTransfer(anyInt(), anyInt(),
				anyInt(), anyInt(),any(XferList.class), anyInt()))
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

}
