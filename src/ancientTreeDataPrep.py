import functions
import pandas as pd
import numpy as np
import pathlib
import os
import json

baseDir=pathlib.Path(__file__).parent.resolve()
parentDir=baseDir.parent
configLocation=os.path.join(baseDir, "config.json")
with open(configLocation, "r") as f:
    configs = json.load(f)

datafolder=os.path.join(parentDir,configs["inputfolder"])
outputfolder=os.path.join(parentDir,configs["outputfolder"])
outputfolderDummy=os.path.join(parentDir,configs["outputfolderDummy"])
sourcefile = os.path.join(datafolder,configs["ati_inputfile"])


# load raw ATI base data (downloaded from https://opendata-woodlandtrust.hub.arcgis.com/datasets)
sourceData=pd.read_csv(sourcefile)

#Get vis polygon geometries
regionpolygons=functions.fetchPolygons( files= {'uki_regionfile': os.path.join(datafolder,configs["uki_regionfile"]), \
                                          'guernsey_regionfile':os.path.join(datafolder,configs["guernsey_regionfile"]), \
                                          'iom_regionfile':os.path.join(datafolder,configs["iom_regionfile"]) }, \
                              outputfolder= os.path.join(datafolder,configs["all_regionfolder"]))

# assign geographic regions based on polygons and x/y
sourceData=functions.assignPolygon(sourceData, regionpolygons)


# create boolean flags for fields with markers
markerDict = configs["markerDict"]
functions.createBoolFlag(sourceData, markerDict)

# Null handling for specific fields - Unknown
fillNAFields=configs["fillNAFields"]
functions.fillnans(sourceData, fillNAFields)


# enforce data types
typeDict=configs["typeDict"]
functions.typeCheck(sourceData, typeDict)

#convert Lat/Long to 8dp (11,8) and store as string (for python only)
sourceData.Latitude=sourceData.Latitude.str.ljust(11, '0')
sourceData.Longitude=sourceData.Longitude.str.ljust(11, '0')
#convert X/Y to 11dp (16,11) and store as string (for python only)
sourceData.x=sourceData.x.str.ljust(17, '0')
sourceData.y=sourceData.y.str.ljust(17, '0')

# Date fields appear to be in mm/dd/yyyy format. No indication that it is inconsistent, therefore we will treat these uniformly
# Set null value as 31/12/9999
functions.fixDates(sourceData, configs["dateFields"])


# Apply grouping to create new higher-level species column. This list generated during EDA - see relevant notebook
species_groups=configs["speciesGroups"]
sourceData['Species']=np.where(sourceData['Species'].str.lower()=='other', 'Unknown',sourceData['Species'] )
sourceData['SpeciesGroup']=sourceData.Species.apply(lambda x:functions.groupSpecies(x, species_groups) )
print('Species grouping complete')

# Create high level flags for Alive status and Ash Dieback
#(whilst AOD/COD (acute/chronic oak decline) appears in the LivingStatus column, the counts are much lower and not split between confirmed/suspected. This field therefore not possible to scale as yet)
sourceData=sourceData.apply(lambda x: functions.livingStatusFlags(x), axis=1)
print('new LivingStatus columns complete')

#create higher level grouping for Public Accessibility
sourceData['PublicAccessibilityGroup']=sourceData.PublicAccessibilityStatus.apply(lambda x: x.split(' ')[0])
sourceData.PublicAccessibilityGroup=np.where(sourceData.PublicAccessibilityGroup.isin(['Public', 'Private']), sourceData.PublicAccessibilityGroup, 'Unknown')
print('Public accessibility groupings complete')

# create marker table with one row per marker per tree (can have multi markers of the same type e.g Fungus)
markerTable=functions.createMarkerTable(sourceData, markerDict)

sourceData=sourceData.drop(columns=markerDict)

#archive existing files
functions.archiveFiles(outputfolder)
functions.archiveFiles(outputfolderDummy)

#save Base Table
functions.saveFile(sourceData, 'ATI_Base_table' , outputfolder, configs["outputFormat"])
#save Marker Table
functions.saveFile(markerTable, 'ATI_Marker_table' , outputfolder, configs["outputFormat"])

#create dummy datasets for Github storage (in place of ATI data)
baseDummy, otherDummy=functions.createDummyFiles(sourceData, [markerTable], indexField='OBJECTID', makeUnknownFields=['RecorderOrganisationName'])
markerDummy=otherDummy[0]
functions.saveFile(baseDummy, 'DUMMY_ATI_Base_table' , outputfolderDummy, configs["outputFormat"])
functions.saveFile(markerDummy, 'DUMMY_ATI_Marker_table' , outputfolderDummy, configs["outputFormat"])