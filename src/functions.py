import pandas as pd
import numpy as np
import geopandas as gpd
import os.path
import datetime as dt
import re 


def fillnans(data, fields):
    """For each of the listed fields, replace nulls (in place) in the base dataset with 'Unknown'

    Args:
        data (DataFrame): Base data table within which nulls are to be replaced with 'Unknown'
        fields (list): list of fields which need 'Unknown' as nulls
    """
    for each in fields:
        data[each]=data[each].fillna('Unknown')
    print('Null handling complete')
        
        
def typeCheck(data, types_dict):
    """Ensure all base data columns align with target data types

    Args:
        data (DatFrame): Base data which is to have data types set
        types_dict (dictionary): lookup between column name and required data type
    """
    for key in types_dict:
        data[key]=data[key].astype(types_dict[key])
    print ('Type conversion complete')


def groupSpecies(rowstring, grouplist):
    """Given a list of common species families (data-led not taxonomy-led) and the non-standardised listed species of a tree record, identify the higher family grouping - if any

    Args:
        rowstring (str): listed species of the tree (human-inputted)
        grouplist (list): accepted list of tree families 

    Returns:
        str: identified tree family, or the original species
    """
    # if available, take intersecting term, if not available, take species raw, if more than one term, take last term in species name as that is generally the actual tree type rather than variant
    intersect=[word for word in rowstring.lower().split(' ') if word in grouplist]
    if len(intersect)>1:
        intersect=[intersect[-1]]
    elif len(intersect)==0:
        intersect=[rowstring]
    return str(intersect[0]).capitalize()


def createBoolFlag(data, fields):
    """For a given list of fields, which in their raw form are concatenated markers, generate boolean flag columns indicating whether any markers are listed

    Args:
        data (DataFrame): Base data with which to generate flags    
        fields (dictionary or list): field names for which to create boolean flags
    """
    for each in fields:
        colname=each+'Flag'
        data[colname]=np.where(data[each].isnull(), 0, 1)
    print ('Boolean flags generated for '+', '.join(fields))


def livingStatusFlags(row):
    """Lambda function - Use the LivingStatus information (single-choice field with free-form 'other' text) to generate Alive/Dead categorical column and also generate Ashdieback column

    Args:
        row (DataFrame row): including LivingStatus and SpeciesGroup (both text)

    Returns:
        DataFrame row: original row with additional columns LivingGroup and AshDieback
    """
    testcase=row.LivingStatus.lower()
    if 'alive' in testcase:
        row['LivingGroup']='Alive'
    elif 'dead' in testcase:
        row['LivingGroup']='Dead'
    else:
        row['LivingGroup']='Unknown'

    if 'chalara fraxinea' in testcase:
        if 'confirmed' in testcase:
            row['AshDieback']='Confirmed'
        else:
            row['AshDieback']='Suspected'
    elif 'Ash' in row.SpeciesGroup:
        row['AshDieback']='Unknown'
    else:
        row['AshDieback']='N/A'

    return row
        
def fixDates(data, datecols, null_value='12/31/9999 12:00:00 AM'):
    """Convert date format from US to UK format and add null handling. In-place correction of date columns

    Args:
        data (DataFrame): base data including at least one date column in US format
        datecols (list): date field names to be reformatted
        null_value (str, optional): Value with which to replace nulls. Defaults to '12/31/9999 12:00:00 AM'.
    """
    for each in datecols:
        # Date fields appear to be in mm/dd/yyyy format. No indication that it is inconsistent, therefore we will treat these uniformly
        # Set null value as 31/12/9999
        data[each]=np.where(data[each]=='nan', null_value, data[each])
        data[each]=data[each].apply(lambda x:  dt.datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p').strftime('%d/%m/%Y'))



def makePivot(data, col, delim):
    """Given a dataset with a target column containing concatenated text markers, perform a split based on given delimiter to create many columns, 
    then perform a melt to convert this into a long (instead of wide) format with one row for every listed value for every original tree (Id). 
    Trees with no markers will not exist in the output table.

    Args:
        data (DataFrame): Base data, one record per tree (Id)
        col (str): name of the column to split/pivot
        delim (str): the text delimiter used to list markers in the given column, used to perform a split operation

    Returns:
        DataFrame: long-format dataframe with tree columns: Id, value, variable
    """
    #assumes column has already had null handling and data type applied so filters based on null strings
    subset=data[~data[col].isin(['Unknown', 'nan'])].copy()
    pivot=pd.melt(pd.concat([subset.Id, subset[col].str.split(delim, expand=True)], axis=1), id_vars=['Id'])
    pivot=pivot[~pivot.value.isnull()].drop_duplicates()
    pivot.variable=col
    return pivot

def createMarkerTable(data, colDict):
    """Given a dataset containing text attribute columns composed of concatenated markers, create a long-format table with one row for every marker value across each attribute per tree
        This concatenates the DataFrame outputs of the makePivot function.

    Args:
        data (DataFrame): Base data, one record per tree (Id), with attribute columns containing concatenated markers
        colDict (dictionary): lookup of column names and the delimiters used to separate each marker within them

    Returns:
        DataFrame: long-form pivot table with one row per marker value, per attribute, per tree (Id)
    """
    output=pd.DataFrame(columns=['Id', 'variable', 'value'])
    for each in colDict:
        pivot=makePivot(data, each, colDict[each])
        output=pd.concat([output, pivot], axis=0)
    output=output.rename(columns={'variable': 'MarkerType', 'value':'MarkerValue'})
    return output



def fetchPolygons(files={'uki_regionfile':'./Data/AncientTrees/Europe_NUTs_2021/NUTS_RG_20M_2021_3035.shp', 'guernsey_regionfile':'./Data/AncientTrees/princeton_Guernsey_shapefile/GGY_adm0.shp', 'iom_regionfile':'./Data/AncientTrees/stanford_IoM_shapefile/nk743nh6214.shp'}, outputfolder='./Data/AncientTrees/VisRegions/', outputfile='region_polygons.shp'):
    """If combined shapefile with all target regions exists, load it into a GeoDataFrame. Otherwise, use given shape file download locations to create a combined set of regions covering the target area of the Ancient Tree Inventory. 
    These inputs cover the NUTs regions of all of Europe (2021) - which needs to be filtered for the UK & Ireland, a shapefile for the Isle of Man, and a shapefile of Guernsey
    Note - shape files must be pre-downloaded, cannot be scraped. 
    Output is saved as shapefile (.shp) to chosen location.

    File sources:
        NUTs Europe 2021 : https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts
        Isle of Man : https://maps.princeton.edu/catalog/stanford-nk743nh6214
        Guernsey : https://maps.princeton.edu/catalog/stanford-bk868xv4713


    Args:
        files (dict, optional): Locations for pre-downloaded shape files covering NUTs (Europe), Isle of Man, and Guernsey. Defaults to {'uki':'./Data/AncientTrees/All_Nuts_2021/NUTS_RG_20M_2021_3035.shp', 'Guernsey':'./Data/AncientTrees/princeton_Guernsey_shapefile/GGY_adm0.shp', 'IoM':'./Data/AncientTrees/stanford_IoM_shapefile/nk743nh6214.shp'}.
        outputfolder (str, optional): _description_. Defaults to './Data/AncientTrees/VisRegions/'.
        outputfile (str, optional): _description_. Defaults to 'region_polygons.shp'.

    Returns:
        GeoDataFrame: Geodataframe containing required regions, ID/Name/Country and polygon geometry in the British National Grid crs (EPSG:27700)
    """
    if os.path.exists(outputfolder+outputfile) :
        regionPolygons=gpd.read_file(outputfolder+outputfile)

    else:
        # see other documentation for the original location of these files
        uki=gpd.read_file(files['uki_regionfile'])
        uki=uki[(((uki.CNTR_CODE=='IE')|(uki.NUTS_ID.str.contains('UKM')))&(uki.LEVL_CODE==2))|((uki.CNTR_CODE=='UK')&(uki.LEVL_CODE==1)&(uki.NUTS_ID!='UKM'))].reset_index()
        IoM=gpd.read_file(files['iom_regionfile'])
        Guernsey=gpd.read_file(files['guernsey_regionfile'])
        # Ensure all are in same british national grid projection 27700, which matches x and y in ATI data
        uki.crs='EPSG:3035'
        uki=uki.to_crs('EPSG:27700')
        IoM.crs='EPSG:4326'
        IoM=IoM.to_crs('EPSG:27700')
        Guernsey.crs='EPSG:4326'
        Guernsey=Guernsey.to_crs('EPSG:27700')
    
        #Combine into one list
        cols=['NUTS_ID', 'CNTR_CODE', 'NUTS_NAME', 'geometry']
        IoM=IoM.rename(columns={'name_fao': 'NUTS_NAME', 'iso':'NUTS_ID'})
        IoM['CNTR_CODE']=IoM.NUTS_ID
        Guernsey=Guernsey.rename(columns={'NAME_ENGLI': 'NUTS_NAME', 'ISO':'NUTS_ID'})
        Guernsey['CNTR_CODE']=Guernsey.NUTS_ID
        regionPolygons=pd.concat([uki[cols], IoM[cols], Guernsey[cols]],ignore_index=True, axis=0)

        # get Country
        regionPolygons['Country']=regionPolygons.apply(lambda x: ['Replublic of Ireland'] if x.CNTR_CODE=='IE' else ['Scotland'] if 'UKM' in x.NUTS_ID else
                 re.findall(r'England|Scotland|Wales|Northern Ireland|Isle of Man|Guernsey+', x.NUTS_NAME), axis=1)
        regionPolygons['Country']=regionPolygons.Country.apply(lambda x: 'England' if len(x)==0 else x[0] )
        regionPolygons['CountryHL']=np.where(regionPolygons.Country.isin(['England', 'Scotland', 'Wales', 'Northern Ireland']), 'UK', regionPolygons.Country)
        regionPolygons=regionPolygons.rename(columns={'NUTS_NAME': 'RegionName', 'NUTS_ID': 'RegionID'})

        if not  os.path.exists(outputfolder) :
            os.makedirs(outputfolder)
        regionPolygons.to_file(outputfolder+outputfile)

    return regionPolygons


def assignPolygon(data, polygons):
    """Given a dataset with  x/y coordinates in the BNG projection (EPSG:27700), classify each datapoint based on the regional polygon it is within.
    For datapoints which don't sit within the bounds of any regional polygon (e.g. on the coast), find the closest polygon

    Args:
        data (DataFrame): Base data including Id and x/y coordinates in EPSG:27700 projection
        polygons (GeoDataFrame): including all regional polygons chosen for visualisation, with columns NUTS_ID, NUTS_NAME, CNTR_CODE and geometry (in EPSG:27700)

    Returns:
        DataFrame: original dataframe with redundant geo-columns removed (Town, County) and updated region / Country information. Country represents lower level detail e.g. Wales, Country_HL repreents higher level e.g. UK
    """
    
    tempdata=gpd.GeoDataFrame(data[['Id']], geometry= gpd.points_from_xy(data.x, data.y))
    tempdata['RegionID']='N/A'

    generate_df=pd.DataFrame(columns=tempdata.columns)

    # cycle through the region polygons, add those within polygon to output dataframe on each loop
    for each in polygons.index:
        out=tempdata.geometry.within(polygons.loc[each, 'geometry'])
        tempdata.loc[out, 'RegionID']=polygons.loc[each, 'RegionID']
        generate_df=pd.concat([generate_df, tempdata[out]], axis=0)
        tempdata=tempdata.loc[~out].copy()

    # for those trees which don't fall within the exact polygon, get the nearest polygon (this is likely due to granularity of the data, mostly due to bein on the coast)
    # we only use this approach for those which don't fall within as it means less computation
    for each in polygons.index:
        poly_id=polygons.loc[each, 'RegionID']
        # column per polygon with distance to
        tempdata[poly_id]=tempdata.geometry.distance(polygons.loc[each, 'geometry'])
    # get polygon name for column with lowest distance
    tempdata['RegionID']=tempdata[polygons.RegionID.unique()].idxmin(axis="columns")

    # add these into the main output data 
    generate_df=pd.concat([generate_df, tempdata[generate_df.columns]], axis=0)
    generate_df=generate_df.merge(polygons[['RegionName', 'RegionID', 'CNTR_CODE', 'Country', 'Country_HL']], on='RegionID', how='left')

    # original Country field to be replaced by polygon mapping and County/Town not required for analysis / not well populated
    data=data.drop(columns=['Country', 'County', 'Town'])
    # join new info into original dataset
    data=data.merge(generate_df[['Id', 'RegionID', 'RegionName', 'Country', 'Country_HL']], on=['Id'], how='left')
    return data


def getTown(row, geocoder):
    """Use reverse geocoding to get the most appropriate level of detail equivalent to 'Town' for each row. 
    As different information is returned for each location, the following hierarchy is used to identify Town equivalent (based on those observed in the raw data):
    village > hamlet > suburb > city_district > city
    Don't use this on an overall scale as would take a LONG time and the existing data doesn't allow for robust analysis. Just for exploring individual locations!

    Args:
        row (DataFrame row): Including Latitude and Longitude fields
        geocoder (GeoCoder): Intialised geocoder using GeoPy (Nominatim recommended)

    Returns:
        str: selected name of 'Town' or nearest estimation
    """
    # Don't use this on an overall scale as would take a LONG time and the existing data doesn't allow for robust analysis. Just for exploring individual locations!
    raw_address=geocoder.reverse(str(row.Latitude)+","+str(row.Longitude)).raw['address']
    out='Unknown'
    for each in ['village', 'hamlet', 'suburb', 'city_district', 'city']:
        if each in raw_address:
            out=raw_address[each]
            break
    return out
    
def getLocation(row, geocoder, uk_countries):
    """ LEGACY - NOT IN USE 
    Row-level function for use in DataFrame lambda. Clean existing County/Country fields where wrong level of detail found, and fetch geospatial attributes using reverse geocoding where fields are not populated.

    Args:
        row (Dataframe row): Including Country, County, Latitude and Longitude
        geocoder (_type_): initialised geocoder - Geopy Nominatim is used here
        uk_countries (_type_): list of acceptable counties which all outputs must fall within (in this case, scraped list)

    Returns:
        row: Corrected / fetched county and country data per row
    """
    #LEGACY - demonstration of use of reverse geocoding to get data, however this alone is not feasible on the number of poor-quality records we have!
    # assumes type conversion already done so filters based on string nulls
    country=str(row.Country)
    county=str(row.County)
    if county in ['NaN','Unknown', 'Other'] or country in ['NaN','Unknown', 'Other'] or (country not in uk_countries):
        raw_address=geocoder.reverse(str(row.Latitude)+","+str(row.Longitude)).raw['address']

        if 'county' in raw_address:
            county=raw_address['county']
        else:
            county='Unknown'

        if 'state' in raw_address : # for UK, country=UK and state = specific country within UK and this aligns with Country in ATI
            country=raw_address['state']
        elif 'country' in raw_address: # for some eg. Isle of Man, state not present but country is
            country=raw_address['country']
        else:
            country='Unknown'
    
    elif country=='Co Wicklow':
        country='Republic of Ireland'
        county='County Wicklow'
    elif 'Ireland' in country and str(country)[0]=='N':
        country='Northern Ireland'
    elif country=='Braddan':
        country='Isle of Man'

    row.Country=country
    row.County=county
    return row