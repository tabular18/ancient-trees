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
markerDict={'Protection':',','Epiphyte':',', 'Fungus':',', 'Condition':',', 'SpecialStatus':',', 'Surroundings':',' }
functions.createBoolFlag(sourceData, markerDict)

# Null handling for specific fields - Unknown
fillna_fields=['StandingStatus', 'LivingStatus', 'RecorderOrganisationName','LocalName','PublicAccessibilityStatus', 'Protection', 'SpecialStatus',  'TreeForm', 'Species']
functions.fillnans(sourceData, fillna_fields)


# enforce data types
typeDict={
'OBJECTID': str , 
'Id': str , 
'Species': str, 
'TreeForm': str, 
'Latitude': str,
'Longitude': str,
'x': str,
'y': str,
'RecorderOrganisationName': str,
'LocalName': str, 
'Country': str, 
'CountryHL': str, 
'RegionID': str, 
'RegionName': str, 
'StandingStatus': str,
'LivingStatus': str, 
'PublicAccessibilityStatus': str, 
'VeteranStatus': str,
'Condition': str, 
'Surroundings': str, 
'Protection': str, 
'SpecialStatus': str, 
'Epiphyte': str,
'Fungus': str,
'SurveyDate': str # for further formatting prior to datetime
}

functions.typeCheck(sourceData, typeDict)

#convert Lat/Long to 8dp (11,8) and store as string (for python only)
sourceData.Latitude=sourceData.Latitude.str.ljust(11, '0')
sourceData.Longitude=sourceData.Longitude.str.ljust(11, '0')
#convert X/Y to 11dp (16,11) and store as string (for python only)
sourceData.x=sourceData.x.str.ljust(17, '0')
sourceData.y=sourceData.y.str.ljust(17, '0')

# Date fields appear to be in mm/dd/yyyy format. No indication that it is inconsistent, therefore we will treat these uniformly
# Set null value as 31/12/9999
functions.fixDates(sourceData, ['SurveyDate', 'VerifiedDate'])


# Apply grouping to create new higher-level species column. This list generated during EDA - see relevant notebook
species_groups=['oak', 'beech', 'cedar', 'lime', 'walnut', 'ash', 'alder',
       'hawthorn', 'willow', 'larch', 'elm', 'poplar', 'cherry',
       'service', 'apple', 'juniper', 'mulberry', 'birch', 'sycamore',
       'maple', 'chestnut', 'pear', 'plane', 'cypress', 'plum', 'yew',
       'laburnum', 'pine', 'whitebeam', 'fir', 'buckthorn']
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

import datetime as dt
now=dt.datetime.now().strftime("%d-%m-%Y_%H%M")

#archive existing files
functions.archiveFiles(outputfolder)
#save 
base_output_file=os.path.join(outputfolder,f'ATI_Base_table_{now}.csv')
marker_output_file=os.path.join(outputfolder,f'ATI_Marker_table_{now}.csv')

sourceData.to_csv(base_output_file,index=False)
print(f'Base table saved with {len(sourceData)} tree records saved to {base_output_file}')
markerTable.to_csv(marker_output_file,index=False)
print(f'Marker table saved with {len(markerTable)} markers across {markerTable.Id.nunique()} trees saved to {marker_output_file}')
