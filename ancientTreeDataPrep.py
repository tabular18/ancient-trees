import functions
import pandas as pd
import numpy as np

from geopy.geocoders import Nominatim

sourcefile='./Data/AncientTrees/Ancient_Tree_Inventory_ATI_-1118788141033660175.csv'
sourceData=pd.read_csv(sourcefile)

#Get vis polygon geometries
polys=functions.fetchPolygons()

# assign geographic regions based on polygons and x/y
sourceData=functions.assignPolygon(sourceData, polys)


# create boolean flags for fields with markers
markerDict={'Protection':',','Epiphyte':',', 'Fungus':',', 'Condition':',', 'SpecialStatus':',', 'Surroundings':',' }
functions.createBoolFlag(sourceData, markerDict)

# Null handling for specific fields - Unknown
fillna_fields=['StandingStatus', 'LivingStatus', 'PublicAccessibilityStatus', 'Protection', 'SpecialStatus',  'TreeForm', 'Species']
functions.fillnans(sourceData, fillna_fields)


# enforce data types
typeDict={
'Id': int , 
'Species': str, 
'TreeForm': str, 
'RecorderOrganisationName': str,
'LocalName': str, 
'Country': str, 
'Country_HL': str, 
'NUTS_ID': str, 
'NUTS_NAME': str, 
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

# Date fields appear to be in mm/dd/yyyy format. No indication that it is inconsistent, therefore we will treat these uniformly
# Set null value as 31/12/9999
functions.fixDates(sourceData, ['SurveyDate', 'VerifiedDate'])


# Apply grouping to create new higher-level species column
species_groups=['oak', 'beech', 'cedar', 'lime', 'walnut', 'ash', 'alder',
       'hawthorn', 'willow', 'larch', 'elm', 'poplar', 'cherry',
       'service', 'apple', 'juniper', 'mulberry', 'birch', 'sycamore',
       'maple', 'chestnut', 'pear', 'plane', 'cypress', 'plum', 'yew',
       'laburnum', 'pine', 'whitebeam', 'fir', 'buckthorn']
sourceData['species_group']=sourceData.Species.apply(lambda x:functions.groupSpecies(x, species_groups) )
print('Species grouping complete')

# Create high level flags for Alive status and Ash Dieback
#(whilst AOD/COD (acute/chronic oak decline) appears in the LivingStatus column, the counts are much lower and not split between confirmed/suspected. This field therefore not possible to scale as yet)
sourceData=sourceData.apply(functions.livingStatusFlags, axis=1)
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

#save 
base_output_file=f'./Data/AncientTrees/ATI_Base_table_{now}.csv'
marker_output_file=f'./Data/AncientTrees/ATI_Marker_table_{now}.csv'
sourceData.to_csv(base_output_file,index=False)
print(f'Base table saved with {len(sourceData)} tree records saved to {base_output_file}')
markerTable.to_csv(marker_output_file,index=False)
print(f'Marker table saved with {len(markerTable)} markers across {markerTable.Id.nunique()} trees saved to {marker_output_file}')
