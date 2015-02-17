About

===============================================================================

This is a Java implementation of the CDVC Media Storms framework, presented in "D. Rafailidis, S. Antaris and Yannis Manolopoulos , Processing Media Storms in the Cloud based on Image Descriptors’ Dimensions Value Cardinalities [Experiments & Analysis Mini Paper]". The source code reproduces the experiments described in our paper.


Datasets

===============================================================================

Our experimental evaluation was performed on two publicly available datasets of image descriptor vectors. We used the GIST-80M-348d dataset of the Tiny Image Collection and the SIFT-1B-128d of the TEXMEX collection, publicly available at http://horation.cs.nyu.edu/mit/tiny/data/index.html and http://corpus-texmex.irisa.fr, respectively. Each dataset file should contain an image descriptor vector per row and the dimensions of each descriptor should be comma separated. 


Usage

===============================================================================

Our framework was implemented using the OpenStack Apis specifications in order to launch server instances and create containers and objects. The framework's components were implemented in Java and the experiments were conducted using the Nephelae Cloud Infrastructure of the laboratory for Internet Computing (LInC) of University of Cyprus (http://linc.ucy.ac.cy/index.php/infrastructure/63-nephelae-cloud-infrastructure). In our implementation, the HBase distributed data management system was used and the RabbitMQ service was applied for the queue services. 

In our repository, we provide the source code of the proposed CDVC Media Storms framework. In order to run our code, the following steps are required:

1) download and install the OpenStack API in your computer;
2) install your certificates into the Cloud Infrastructure;
3) create the HBase configuration which will be used by each of our framework’s components. The HBase configuration should be included either as a separate cfg file (hbs.cfg) or it should be embedded into the experiment.cfg;
4) create the RabbitMQ configuration. Similar to the HBase configuration, the RabbitMQ configuration should be included either as a separate cfg file (rbmq.cfg) or it should be embedded into the experiment.cfg;
5) create the Openstack Deployment projects for each of our framework's components. Each component is deployed to the Openstack compute node; 
6) modify the numberOfDVCExtractorNodes variable in the experiment.cfg according to the deployed instances of the DVC Extractor instances; 
7) modify the numberOfImageSorterNodes variable in the experiment.cfg according to the deployed instances of the Image Sorte instances;
8) if the numberOfDVCExtractorNodes and numberOfImageSorterNodes variables are assigned as 0, then the sequential approach is executed.

We provide in our repository a CDVC_STORM_Initializer project which is the user interface of our framework. CDVC_STORM_Initializer should not deployed to the cloud but it should include the Openstack libraries, since it interacts with the Cloud infrastructure. Since the framework's components are deployed to the cloud, you need to execute the CDVC_STORM_Initializer to run our experiments.




