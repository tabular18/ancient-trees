# ancient-trees
Welcome to this repository for analysis of the Ancient Tree Inventory! The inventory is a citizen science project owned and managed by the Woodland Trust, with the intention of collecting information on ancient, veteran and important trees across the UK and Republic of Ireland.
Data reproduced with the permission of The Woodland Trust, accessed 09/2023.

## Environment
For the purposes of this project, a virtual environment has been used with Python 3.11.5. The list of all packages installed within this environment is available in requirements.txt, and this is conducive to running the python and ipython notebooks stored in this repo.

## Exploratory Analysis

## Target Table Pipeline
The processing pipeline is available in the src folder, and is split across three files: config.json (storing variables such as document locations), functions.py (storing modular functions used in the data prep) and ancientTreeDataPrep.py (the main data prep process).
The data prep pipeline takes the following datasets as input:
- a raw copy of the [ancient tree inventory](https://opendata-woodlandtrust.hub.arcgis.com/datasets/)
- either of:
    - three shapefiles, covering the [Isle of Man](https://maps.princeton.edu/catalog/stanford-nk743nh6214), [Guernsey](https://maps.princeton.edu/catalog/stanford-bk868xv4713) and all designated [NUTs regions across Europe](https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts)
    - a compiled shapefile covering the relevant NUTs regions for this analysis (UK & RoI) and the Isle of Man and Guernsey. This file is generated upon first run of the pipeline file if it doesn't already exist (note - it is available in the repo)

The output of the data prep pipeline is two tables - the Base table (one record per tree) and the Marker table (one record per marker value, per marker type, per tree). The former is identical granularity to the raw ATI data, whilst the latter is a pivotted view (e.g. 'long') of the markers which are concatenated in the original dataset. An example of this is a tree with multiple awards listed in the SpecialStatus column - this will be outputted as two rows, one per award, for the given tree.

This data model format ensures that text-heavy marker data can be stored separately and only referenced when required in visualisations. Both Power BI and Tableau use data which is 'related' but in separate tables in this manner, allowing for efficiency in visualisation calculations. Having fewer columns but more rows also allows for efficient storage. Outputs are stored in data/output

To run the pipeline, ensure a python environment is available (and activated, if using a virtualenv!) and has the requirements.txt list of modules installed. Then use the following command (windows cmd) python AncientTrees/src/ancientTreeDataPrep.py

## Visualisation


