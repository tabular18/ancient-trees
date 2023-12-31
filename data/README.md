# Data Notes

```
   ancient-trees
   └── data
       ├── input
       |   ├── ATIdownload/
       |   ├── VisRegions/
       |   ├── Europe_NUTs_2021_shapes/
       |   ├── Guernsey_shapes/
       |   └── Isle_of_Man_shapes/
       └── output
           ├── dummy
           |   ├──DUMMY_ATI_Base_table_<timestamp>.csv
           |   ├──DUMMY_ATI_Marker_table_<timestamp>.csv
           |   └──archive/
           └── actual
               ├──ATI_Base_table_<timestamp>.csv
               ├──ATI_Marker_table_<timestamp>.csv
               └──archive/


```

The data pipeline process (ancientTreeDataPrep.py) uses a configuration file to access key information such as input/output file paths. Any changes to input/output locations should be accompanied by updates to this config file to reflect.

It is important to note that the original input data - e.g. the raw Ancient Tree Inventory data - is not stored in the remote repository. To run the pipeline locally, data should be downloaded from the [Woodland Trust open data hub](https://opendata-woodlandtrust.hub.arcgis.com/datasets/). 

Once downloaded, this should be saved to the data/input/ATIdownload folder as a csv. The location of this dataset is then noted in the config file - ensure that the 'ati_inputfile' item is updated to reflect any differing filename used.

As we are using Tableau, output files are generally stored as CSV - for other purposes, the pipeline can also be used to create parquet output files. Note that parquet formats can be opened in Tableau using third-party CData plug-in, but this has not been trialled during this project. Notable advantages of parquet format are 1) storage efficiency and 2) the ability to store datatype and other column meta-data.

Upon running the data pipeline, any previously stored outputs are archived - that is, moved into an 'archive/' sub-folder which is not tracked - e.g. to allow us to retain previous runs locally but not remotely, keeping the remote repository clean.

In addition, only dummy copies of the output tables are tracked in the repository, to prevent publishing of the full ATI dataset outside of the Woodland Trust hub. Locally, the full datasets (e.g. those loaded into the Tableau backend) are stored into a 'actual' folder in parallel with dummy datasets being stored to the 'dummy' folder visible in the remote repo. Use these dummy files to understand the output file structure which reflects the target data model discussed in the [project overview document.](./docs/ProjectSummary.pdf)
