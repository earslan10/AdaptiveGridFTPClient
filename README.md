
# Dynamic Protocol Tuning Algorithms
1. Import the project to Intellij as a Maven Project, you will need to download all the dependencies which you can do as follows.  
     a) Press Ctrl + E to open recent files  
     b) Open Maven Projects  
     c) Click on install, this way you will install all the dependencies required for this code.  
     d) If you don't have Intellij use "run package" command to download all the dependencies.  
  
2. Edit configurations in config.cfg in src/main/resources folder as follows

  **-s** $Source_GridFTP_Server  
  **-d** $Destination_GridFTP_Server  
  **-proxy** $Proxy_file_path (Default will try to read from /tmp for running user id)  
  **-cc** $maximum_allowed_concurrency  
  **-rtt** $rtt (round trip time between source and destination) in ms  
  **-bw** $bw (Maximum bandwidth between source and destination) in Gbps  
  **-bs** $buffer_size (TCP buffer size of minimum of source's read and destination's write in MB)  
  **[-single-chunk]** (Will use Single Chunk [SC](http://dl.acm.org/citation.cfm?id=2529904) approach to schedule transfer. Will transfer one chunk at a time)  
  **[-useHysterisis]** (Will use historical data to run modelling and estimate transfer parameters. [HARP]. Requires python to be installed with scipy and sklearn packages)  
  **[-use-dynamic-scheduling]** (Provides dynamic channel reallocation between chunks while transfer is running [ProMC](http://dl.acm.org/citation.cfm?id=2529904))

  For example:  
  -s gsiftp://gridftp.stampede.tacc.xsede.org:2811/scratch/01814/earslan/mixedDataset/  
  -d gsiftp://oasis-dm.sdsc.xsede.org:2811/oasis/scratch/earslan/temp_project/mixedDataset/  
  -proxy /tmp/x509up_u501  
  -cc 10  
  -rtt 0.04  
  -bw 10  
  -bs 32  
  -useHysterisis  
  -use-dynamic-scheduling

3. Run AdaptiveGridFTPClient.java to start the transfer 
4. Logs will be stored in harp.log and inst-throughput.txt inside the project folder.  
  
*This code is tested with Java 1.8 and Python 2.7  
