import pandas as pd
import numpy as np
import geopandas as gpd
import os.path
import datetime as dt
import re 


def fillnans(data, fields):
    for each in fields:
        data[each]=data[each].fillna('Unknown')
    print('Null handling complete')
        
        
def typeCheck(data, types_dict):
    for key in types_dict:
        data[key]=data[key].astype(types_dict[key])
    print ('Type conversion complete')
    
def getLocation(row, geocoder, uk_countries):
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


def groupSpecies(rowstring, grouplist):
    # if available, take intersecting term, if not available, take species raw, if more than one term, take last term in species name as that is generally the actual tree type rather than variant
    intersect=[word for word in rowstring.lower().split(' ') if word in grouplist]
    if len(intersect)>1:
        intersect=[intersect[-1]]
    elif len(intersect)==0:
        intersect=[rowstring]
    return str(intersect[0]).capitalize()


def createBoolFlag(data, fields):
    for each in fields:
        colname=each+'_flag'
        data[colname]=np.where(data[each].isnull(), 0, 1)
    print ('Boolean flags generated for '+', '.join(fields.keys()))


def livingStatusFlags(row):
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
    elif 'Ash' in row.species_group:
        row['AshDieback']='Unknown'
    else:
        row['AshDieback']='N/A'

    return row
        
def fixDates(data, datecols, null_value='12/31/9999 12:00:00 AM'):
    for each in datecols:
        # Date fields appear to be in mm/dd/yyyy format. No indication that it is inconsistent, therefore we will treat these uniformly
        # Set null value as 31/12/9999
        data[each]=np.where(data[each]=='nan', null_value, data[each])
        data[each]=data[each].apply(lambda x:  dt.datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p').strftime('%d/%m/%Y'))


def getTown(row, geocoder):
    # Don't use this on an overall scale as would take a LONG time and the existing data doesn't allow for robust analysis. Just for exploring individual locations!
    raw_address=geocoder.reverse(str(row.Latitude)+","+str(row.Longitude)).raw['address']
    out='Unknown'
    for each in ['village', 'hamlet', 'suburb', 'city_district', 'city']:
        if each in raw_address:
            out=raw_address[each]
            break
    return out

def makePivot(data, col, delim):
    #assumes column has already had null handling and data type applied so filters based on null strings
    subset=data[~data[col].isin(['Unknown', 'nan'])].copy()
    pivot=pd.melt(pd.concat([subset.Id, subset[col].str.split(delim, expand=True)], axis=1), id_vars=['Id'])
    pivot=pivot[~pivot.value.isnull()].drop_duplicates()
    pivot.variable=col
    return pivot

def createMarkerTable(data, colDict):
    output=pd.DataFrame(columns=['Id', 'variable', 'value'])
    for each in colDict:
        pivot=makePivot(data, each, colDict[each])
        output=pd.concat([output, pivot], axis=0)
    return output



def fetchPolygons(files={'uki':'./Data/AncientTrees/All_Nuts_2021/NUTS_RG_20M_2021_3035.shp', 'Guernsey':'./Data/AncientTrees/princeton_Guernsey_shapefile/GGY_adm0.shp', 'IoM':'./Data/AncientTrees/stanford_IoM_shapefile/nk743nh6214.shp'}, outputfolder='./Data/AncientTrees/VisRegions/', outputfile='region_polygons.shp'):
    if os.path.exists(outputfolder+outputfile) :
        regionPolygons=gpd.read_file(outputfolder+outputfile)

    else:
        # see other documentation for the original location of these files
        uki=gpd.read_file(files['uki'])
        uki=uki[(((uki.CNTR_CODE=='IE')|(uki.NUTS_ID.str.contains('UKM')))&(uki.LEVL_CODE==2))|((uki.CNTR_CODE=='UK')&(uki.LEVL_CODE==1)&(uki.NUTS_ID!='UKM'))].reset_index()
        IoM=gpd.read_file(files['IoM'])
        Guernsey=gpd.read_file(files['Guernsey'])
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
        regionPolygons['Country_HL']=np.where(regionPolygons.Country.isin(['England', 'Scotland', 'Wales', 'Northern Ireland']), 'UK', regionPolygons.Country)

        if not  os.path.exists(outputfolder) :
            os.makedirs(outputfolder)
        regionPolygons.to_file(outputfolder+outputfile)

    return regionPolygons


def assignPolygon(data, polygons):
    # requires input dataframe which has 
    tempdata=gpd.GeoDataFrame(data[['Id']], geometry= gpd.points_from_xy(data.x, data.y))
    tempdata['NUTS_ID']='N/A'

    generate_df=pd.DataFrame(columns=tempdata.columns)

    # cycle through the region polygons, add those within polygon to output dataframe on each loop
    for each in polygons.index:
        out=tempdata.geometry.within(polygons.loc[each, 'geometry'])
        tempdata.loc[out, 'NUTS_ID']=polygons.loc[each, 'NUTS_ID']
        generate_df=pd.concat([generate_df, tempdata[out]], axis=0)
        tempdata=tempdata.loc[~out].copy()

    # for those trees which don't fall within the exact polygon, get the nearest polygon (this is likely due to granularity of the data, mostly due to bein on the coast)
    # we only use this approach for those which don't fall within as it means less computation
    for each in polygons.index:
        poly_id=polygons.loc[each, 'NUTS_ID']
        # column per polygon with distance to
        tempdata[poly_id]=tempdata.geometry.distance(polygons.loc[each, 'geometry'])
    # get polygon name for column with lowest distance
    tempdata['NUTS_ID']=tempdata[polygons.NUTS_ID.unique()].idxmin(axis="columns")

    # add these into the main output data 
    generate_df=pd.concat([generate_df, tempdata[generate_df.columns]], axis=0)
    generate_df=generate_df.merge(polygons[['NUTS_NAME', 'NUTS_ID', 'CNTR_CODE', 'Country', 'Country_HL']], on='NUTS_ID', how='left')

    # original Country field to be replaced by polygon mapping and County/Town not required for analysis / not well populated
    data=data.drop(columns=['Country', 'County', 'Town'])
    # join new info into original dataset
    data=data.merge(generate_df[['Id', 'NUTS_ID', 'NUTS_NAME', 'Country', 'Country_HL']], on=['Id'], how='left')
    return data