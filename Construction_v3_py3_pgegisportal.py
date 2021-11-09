#=============================================================================
# INPUTS
#=============================================================================

DOTHEQC = False 
DOTHEAUTOID = True 
DOWNLOADKMZ = False
DOTHEARCHIVE = True
kmzfolder = r'\\rcshare01-nas\EncroachmentManagement\4_GTVM_Construction\0_Master_Construction_Deliverables\Current_AGOL_Projects'
backupfolder = None 
logfile = r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Mapping\AGOL_Active_Services\02_Export_Logs\Export_Logs.gdb\CONSTRUCTION'


#=============================================================================





def handle_dupes_get_max(features, auto_id_field, created_date_field, prefix):
    print('checking for dupes')
    uniques = []
    dupes = []
    nums = []
    for feature in features:
        aid = feature.get_value(auto_id_field)
        if aid is not None:
            if aid in uniques:
                dupes.append(aid)
            else:
                uniques.append(aid)
            if prefix in aid:
                nums.append(int(aid.split('_')[1]))
    if dupes:
        print('\tYOU GOT SOME DUPES')
        print('\t{} dupes'.format(len(dupes)))
        dupe_dict ={dupe:[feature for feature in features if feature.get_value(auto_id_field)==dupe] for dupe in dupes}
        for dupe in dupe_dict:
            try:
                first_date = min([feature.get_value(created_date_field) for feature in dupe_dict[dupe]])
                #this assumes that the feature objects are static
                for feature in dupe_dict[dupe]:
                    if feature.get_value(created_date_field) != first_date:
                        feature.attributes[auto_id_field]=None
            except TypeError:
                min_oid = min([feature.get_value('OBJECTID') for feature in dupe_dict[dupe]])
                for feature in dupe_dict[dupe]:
                    if feature.get_value('OBJECTID') != min_oid:
                        feature.attributes[auto_id_field]=None
                
    else:
        print('\tyou got no dupes')
        dupe_dict = {}
    if not nums:
        nums=[0,1]
    return max(nums), dupe_dict


def dupes_auto_id(layer, auto_id_field, prefix, created_date_field):
    
    all_features = layer.query(out_fields = ",".join([layer.properties.globalIdField,
                                                      created_date_field,
                                                      auto_id_field]), return_geometry=False)
    max_num, dupe_dict = handle_dupes_get_max(all_features.features, auto_id_field, created_date_field, prefix)   
    # doesnt return next number
    max_num += 1
    
    # empty list for collecting updates
    updates = []
    for feature in all_features.features:
        if feature.get_value(auto_id_field) == None:
            feature.set_value(auto_id_field, "{}_{}".format(prefix, str(max_num).zfill(5)))
            updates.append(feature)
            max_num += 1
            
    return updates


def check_project_ids(project_layer, input_layer, proj_id_field, include_fields = []):
    
    include_fields+=[input_layer.properties.globalIdField, proj_id_field]
    
    sdf_all_projects = project_layer.query(out_fields = proj_id_field).sdf
    sdf_all_input = input_layer.query(out_fields = ",".join(include_fields)).sdf
    
    sdf_join =  sdf_all_input.spatial.join(sdf_all_projects, left_tag = 'inv', right_tag = 'proj')
    
    return sdf_join[sdf_join['PROJECT_inv']!=sdf_join['PROJECT_proj']]


def local_sj(layer, sdf_local, field_maps, key_field):
    #Get feature count and sr
    fset_needs_fields = layer.query(where=" IS NULL OR ".join([field_map[0] for field_map in field_maps])+" IS NULL",
                                    out_fields = ",".join([field_map[0] for field_map in field_maps]))  
    if len(fset_needs_fields.features)==0:
        # arcpy.AddMessage("\tNO NULL VALUES")
        return None
    else:
        sdf_agol = fset_needs_fields.sdf
        sdf_join = sdf_agol.spatial.join(sdf_local, left_tag = 'AGOL', right_tag = 'LOCAL')
        if sdf_join.shape[0] == 0:
            return None
        else:
            column_dict = {}
            for field_map in field_maps:
                if field_map[1] in sdf_join.columns:
                    column_dict[field_map[1]] = field_map[0]
                else:
                    column_dict[field_map[1]+"_LOCAL"] = field_map[0]
            
            column_dict['OBJECTID_AGOL'] = 'OBJECTID'
            # droppers = [value + "_AGOL" if value + "_AGOL" in sdf_join.columns else value for value in list(column_dict.values())]
            
            # droppers += ['SHAPE','index_LOCAL','OBJECTID_LOCAL']
            droppers = [column for column in sdf_join.columns if column not in column_dict]
            sdf_join.drop(columns = droppers, inplace = True)
            sdf_join.rename(columns = column_dict, inplace = True)
            
            return sdf_join.spatial.to_featureset()


def att_join_multiple(layer, reflayer, join_field_map, field_maps):
    from pandas import DataFrame
    idfield = join_field_map[0]
   
    sdf_nulls = layer.query(where="("+" IS NULL OR ".join([field[0] for field in field_maps])+" IS NULL) AND "+idfield+" IS NOT NULL",
                            out_fields = ",".join([fm[0] for fm in field_maps]+[join_field_map[0]]),
                            return_geometry = False).sdf
    
    null_to_lookup = sdf_nulls[idfield].tolist()
    sdf_reference = reflayer.query(where = join_field_map[1]+" IN ('"+"','".join(null_to_lookup)+"')",
                                   out_fields = ",".join([fm[1] for fm in field_maps]+[join_field_map[1]]),
                                   return_geometry = False).sdf
  
    sdf_joiner =  sdf_nulls.merge(sdf_reference, 'left',
                          left_on = join_field_map[0],
                          right_on = join_field_map[1],
                          suffixes = ('','REF'))
    dicts_joiner = sdf_joiner.to_dict('records')
    in_fields = [fm[0] for fm in field_maps]+[join_field_map[0]]
    ref_fields = [fm[1] for fm in field_maps]+[join_field_map[1]]
    
    ref_df_fields = [fm[1]+'REF' if fm[0]==fm[1] else fm[1] for fm in field_maps]
    field_maps_df = list(zip(in_fields[0:-1], ref_df_fields))
    
    updaters = []
    for dj in dicts_joiner:
        update = False
        for fmd in field_maps_df:
            if dj[fmd[0]] != dj[fmd[1]]:
                print(fmd)
                print(dj[fmd[0]] , dj[fmd[1]])
                dj[fmd[0]] = dj[fmd[1]]

                update = True
        if update:
            updaters.append(dj)
    df_updates =  DataFrame(updaters)
    #df_updates.drop(columns = ref_df_fields, inplace = True)
    return df_updates#.spatial.to_featureset()
    

def print_message(message):
    print(message)
    arcpy.AddMessage(message)


def check_for_updates(layer, field, date, update_list):
    print_message("Checking for updates in "+layer.properties.name)
    queryset = layer.query(where="""EditDate > date'"""+date+"""'""", return_geometry=False)
    for feature in queryset.features:
        uv = feature.get_value(field)
        if uv not in update_list and uv != None and uv!= ' ' and uv !='':
            update_list.append(uv)


def const_map_kmz(project, layer, out_folder):
    layer.name = project
    for sl in layer.listLayers():
        sl.definitionQuery = "PROJECT = '{}'".format(project)
    templyr = layer.saveACopy(out_folder+"\\"+project)
    kmz = arcpy.conversion.LayerToKML(templyr, os.path.join(out_folder,"{}.kmz".format(project)))
    arcpy.management.Delete(templyr)
    return kmz


def polygon_sqft(layer, sqft_field):
    sdf_layer = layer.query(out_fields = sqft_field, as_df=True)
    sdf_layer.spatial.project(26910)
    sdf_layer['NEW_SQFT'] = sdf_layer['SHAPE'].apply(lambda x: x.area * 10.7639)
    fset = sdf_layer[abs(sdf_layer['NEW_SQFT'] - sdf_layer[sqft_field]) > 1]\
        [['OBJECTID','NEW_SQFT',sqft_field]]\
        .rename(columns={'NEW_SQFT':sqft_field, sqft_field: 'OLD_SQFT'})\
        .spatial.to_featureset()
    return fset

#This archives any reference layer marked Complete
def archive_complete_reference(layer, status_field, archive_field):
    
    reference_features = layer.query(out_fields = ",".join([layer.properties.globalIdField, status_field, archive_field]), return_geometry=False)
    
    updates_ref = []
    for feature in reference_features.features:
        if feature.get_value(status_field) == "Complete" and feature.get_value(archive_field) != "Yes":
            feature.set_value(archive_field, "Yes")
            updates_ref.append(feature)
    
    return updates_ref
    

import datetime
import arcpy
import os
import arcgis
arcpy.env.overwriteOutput = True

# DOTHEQC = arcpy.GetParameter(0) #False 
# DOTHEAUTOID = arcpy.GetParameter(1) #True 
# DOWNLOADKMZ = arcpy.GetParameter(2) #True 
# kmzfolder = arcpy.GetParameterAsText(3) #r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Construction\DatasetsForMapping\New_KMZ_Tests'
# backupfolder = arcpy.GetParameterAsText(4) #None 
# logfile = arcpy.GetParameter(5) #r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Mapping\AGOL_Active_Services\02_Export_Logs\Export_Logs.gdb\CONSTRUCTION'


gis_object = arcgis.GIS('https://pgegisportal.maps.arcgis.com','cc_gtvmaps', 'HappyBlueTuna2021')



itemid_construction = 'f00407c42a304ccf89803e37c1fd1c11'
item_construction = arcgis.gis.Item(gis_object, itemid_construction)

   
lyr_flag = item_construction.layers[0]
lyr_vegpt = item_construction.layers[1]
lyr_brar = item_construction.layers[2]
lyr_exzo = item_construction.layers[3]
lyr_proj = item_construction.layers[4]
lyr_ref = item_construction.layers[5]




if DOTHEQC:
    print_message("THIS HASN'T BEEN IMPLEMENTED YET")
else:
    print_message("You've chosen not to do QC today")

if DOTHEAUTOID:
    print_message('AUTO-ID PROCESSING')
    print_message("Vegetation Points")
    updates_vegpt = dupes_auto_id(lyr_vegpt, "AUTO_ID", "VP", lyr_vegpt.properties.editFieldsInfo['creationDateField'])
    if updates_vegpt:
        print_message('\tApplying Edits for {} features'.format(len(updates_vegpt)))
        lyr_vegpt.edit_features(updates = updates_vegpt)
    else:
        print_message('\tNo new Auto IDs for VegPt')

    print_message("Brush Areas")
    updates_brar = dupes_auto_id(lyr_brar, "AUTO_ID", "BA", lyr_brar.properties.editFieldsInfo['creationDateField'])
    if updates_brar:
        print_message('\tApplying Edits for {} features'.format(len(updates_brar)))
        lyr_brar.edit_features(updates = updates_brar)
    else:
        print_message('\tNo new Auto IDs for Brush Areas')

    print_message("Exclusion Zones")        
    updates_exzo = dupes_auto_id(lyr_exzo, "AUTO_ID", "XZ", lyr_exzo.properties.editFieldsInfo['creationDateField'])
    if updates_exzo:
        print_message('\tApplying Edits for {} features'.format(len(updates_exzo)))
        lyr_exzo.edit_features(updates = updates_exzo)
    else:
        print_message('\tNo new Auto IDs for Exclusion Zones')

    
    df_projects = lyr_proj.query(as_df=True)
    print_message('Updating Inventory Project Values...')
    print_message('\tVegPt')
    updates_proj_pt = local_sj(lyr_vegpt, df_projects, [('PROJECT','PROJECT')],'PROJECT')
    if updates_proj_pt:
        print_message('\tApplying Edits...')
        lyr_vegpt.edit_features(updates = updates_proj_pt.features)
    
    print_message('\tBrush Areas')
    updates_proj_ba = local_sj(lyr_brar, df_projects, [('PROJECT','PROJECT')],'PROJECT')
    if updates_proj_ba:
        print_message('\tApplying Edits...')
        lyr_brar.edit_features(updates = updates_proj_ba.features)
        
    print_message('\tExclusion Zones')
    updates_proj_xz = local_sj(lyr_exzo, df_projects, [('PROJECT','PROJECT')],'PROJECT')
    if updates_proj_xz:
        print_message('\tApplying Edits...')
        lyr_exzo.edit_features(updates = updates_proj_xz.features)


    trans_fieldmaps = [('LEGACY_BASE_ID', 'LEGACY_BASE_ID'), 
                   ('LONG_PROJECT', 'LONG_PROJECT'), 
                   ('ORDER_NUMBER', 'ORDER_NUMBER'), 
                   ('WORKSTREAM', 'WORKSTREAM'), 
                   ('WORKTYPE', 'WORKTYPE'), 
                   ('DIVISION', 'DIVISION'), 
                   ('GTVM_LC', 'GTVM_LC'), 
                   ('SCHEDULELOCATION', 'SCHEDULELOCATION'), 
                   ('ROUTE', 'ROUTE'), 
                   ('YEAR', 'YEAR')]
    id_fieldmap = ('PROJECT','PROJECT')
    # outers = att_join_multiple(lyr_proj, lyr_ref, id_fieldmap, trans_fieldmaps)

    # calc sqft for brush areas
    fset_sqft = polygon_sqft(lyr_brar, 'AREA_SQUARE')
    if fset_sqft.features:
        print_message('Updating SQFT values for {} features'.format(len(fset_sqft.features)))
        lyr_brar.edit_features(updates=fset_sqft.features)
else:
    print_message("You've chosen not to do auto processing today")

if DOWNLOADKMZ:
    print_message("STARTING KMZ CREATION")
    # get the last date from the log file
    transdates = []
    datecursor = arcpy.SearchCursor(logfile)
    for row in datecursor:
        transdates.append(row.getValue('UPDATE_DATE'))
    del datecursor
    dater = max(transdates).strftime('%m/%d/%Y %H:%M:%S')
    
    # get right now to do add into the table later
    rightnow = datetime.datetime.utcnow().strftime('%m/%d/%Y %H:%M:%S')

    # check for any updates to layers since that date in the log file
    print_message ("Checking for updates in AGOL")
    updatedPMOs = []
    check_for_updates(lyr_flag,"PROJECT",dater,updatedPMOs)
    check_for_updates(lyr_vegpt,"PROJECT",dater,updatedPMOs)
    check_for_updates(lyr_exzo,"PROJECT",dater,updatedPMOs)
    check_for_updates(lyr_brar,"PROJECT",dater,updatedPMOs)
    check_for_updates(lyr_proj,"PROJECT",dater,updatedPMOs)
    



    # load the map to make KMZs
    arcproject_path = r"\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Construction\DatasetsForMapping\Construction_Daily_KMZ.aprx"
    arcproject = arcpy.mp.ArcGISProject(arcproject_path)
    #arcproject.importDocument(r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Construction\DatasetsForMapping\Map3_dave.mapx')
    arcproject.importDocument(r'\\rcshare01-nas\EncroachmentManagement\9_ActiveWorkspace\Construction\DatasetsForMapping\Map_Paul2.mapx')
    all_maps = arcproject.listMaps()
    prolyr_group = all_maps[-1].listLayers()[0]
    
    # make KMZs for each updated project
    for project in updatedPMOs:
        print_message(project)
        const_map_kmz(project, prolyr_group, kmzfolder)

    #put the current time in the log file    
    insertor = arcpy.da.InsertCursor(logfile,['TYPE','UPDATE_DATE','UPDATED_PROJECTS'])
    row = ('Transmission', rightnow, ','.join(updatedPMOs)[0:255])
    insertor.insertRow(row)
    del insertor   

else:
    print_message("You've chosen not to do KMZs today")
    
    
if DOTHEARCHIVE:
    print_message('ARCHIVE PROCESSING')
    archive_ref = archive_complete_reference(lyr_ref, "GCVM_1", "Archive")
    if archive_ref:
        print_message('\tApplying Edits for {} features'.format(len(archive_ref)))
        lyr_ref.edit_features(updates = archive_ref)
    else:
        print_message('\tNo new Completed Reference Layers')










