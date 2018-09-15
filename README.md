# Custom GridFTP Client for Parameter Auto-tuning

Implementation of parameter tuning algorithms presented below

Dynamic Protocol Tuning Algorithms for High Performance Data Transfers (Europar'13, JPDC'18)  
  https://link.springer.com/chapter/10.1007/978-3-642-40047-6_72

HARP: Predictive Transfer Optimization Based on Historical Analysis and Real-Time Probing (SC'16, TPDS'18)  
https://ieeexplore.ieee.org/abstract/document/7877103/

## Installation

### Requirements
* Linux
* Java 1.8
* Python 2.7 and up
* mvn

Run following command in base directory  
`$ mvn compile`

## Usage

* Import the project to Intellij as a Maven Project, you will need to download all the dependencies which you can do as follows.  
     a) Press Ctrl + E to open recent files  
     b) Open Maven Projects  
     c) Click on install, this way you will install all the dependencies required for this code.  
     d) If you don't have Intellij use "mvn compile" command to download all the dependencies.  

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
  Sample config file can be found in main/src/resources/confif.cfg

3. Run AdaptiveGridFTPClient.java to start the transfer
4. Logs will be stored in harp.log and inst-throughput.txt inside the project folder.  
