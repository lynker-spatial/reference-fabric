# The loop in the code iterates through each ".gpkg" file in `wb_dir` and performs the following operations on each file:
# 
# 1. Read in the geospatial data as an sf object (`wb`).
# 2. Drop the geometry column (`tmp`).
# 3. Rename the column names to upper case (`names(tmp)`).
# 4. Reattach the geometry column (`wb`).
# 5. Union polygons by the attribute `COMID` and transform to the coordinate reference system with EPSG code 5070 (`xx`).
# 6. Remove holes from the polygons and join with the original water body data based on the `COMID` attribute (`xx`).
# 7. Select specific attributes (`GNIS_ID`, `GNIS_NAME`, `COMID`, `FTYPE`) from `xx`.
# 8. Filter rows where `GNIS_NAME` is NA (`xx1`).
# 9. Filter rows where `GNIS_NAME` is not NA and add a new column `area_sqkm` with the area in square kilometers (`xx2`).
# 10. Union polygons by the attribute `GNIS_NAME` and join with the `xx2` data based on the `GNIS_NAME` attribute (`out`).
# 11. Group by `GNIS_NAME`, create a new column `member_comid` with a comma-separated list of `COMID`s, and select the row with the largest `area_sqkm` (`out`).
# 12. Bind the filtered rows from step 8 (`xx1`) with the selected rows from step 11 (`out`).
# 13. Select specific attributes (`GNIS_ID`, `GNIS_NAME`, `COMID`, `FTYPE`, `member_comid`) from the combined data (`out`).
# 14. Add a new column `area_sqkm` with the area in square kilometers (`out`).
# 15. Write the resulting data to a new ".gpkg" file named based on the VPU name (`outfile`).

# load config.R file to get file paths
source('reference-features/nhdplusv2/config.R')

# Get a list of all ".gpkg" files in the specified directory
all       = list.files(wb_dir, full.names = TRUE, pattern = ".gpkg$")

# WB_COMID == 166410600
# VPU13

# Waterbodies -------------------------------------------------------------

# Loop through each ".gpkg" file in the directory and process water body features
for(i in 1:length(all)){
  
  # Get the path of the current file
  path = all[i]
  
  # Get the VPU name from the file name
  which_VPU = gsub(".gpkg", "", gsub("NHDPlus", "", basename(path)))
  
  # Specify the output file name based on the VPU name
  outfile = glue("{reference_dir}waterbodies_{which_VPU}.gpkg")

  # Only process the file if the output file doesn't exist yet
  if(!file.exists(outfile)){

    # Read in the water body data from the current file as an sf object
    wb  = read_sf(path)

    # Drop the geometry column and rename column names to upper case
    tmp = st_drop_geometry(wb)
    names(tmp) <- toupper(names(tmp))
    
    # Drop Z/M dimensions
    wb = st_zm(
      st_as_sf(
        cbind(tmp, st_geometry(wb))
        )
      )
    
    # Union polygons by the COMID attribute and transform to EPSG code 5070
    xx = 
      wb %>% 
      union_polygons("COMID") %>%
      st_transform(5070) %>%
      sf_remove_holes() %>%
      left_join(st_drop_geometry(wb), by = "COMID") %>%
      select(GNIS_ID, GNIS_NAME, COMID, FTYPE)
    
    # Filter rows where GNIS_ID is NA
    xx1 =
      xx %>% 
      filter(is.na(GNIS_ID)) %>%
      mutate(member_comid = as.character(COMID))
    
    # Filter rows where GNIS_NAME is not NA and add a new area_sqkm column
    xx2 = 
      xx %>% 
      filter(!is.na(GNIS_ID)) %>%
      mutate(area_sqkm = add_areasqkm(.))
    
    # incase no rows are left in XX2, then just save out xx1
    if(nrow(xx2) > 0) {
      out = 
        xx2 %>% 
        union_polygons("GNIS_ID") %>%
        left_join(st_drop_geometry(xx2), by = "GNIS_ID") %>%
        group_by(GNIS_ID) %>%
        mutate(member_comid = paste(COMID, collapse = ",")) %>%
        slice_max(area_sqkm) %>%
        ungroup() %>%
        bind_rows(xx1) %>%
        select(GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid) %>%
        mutate(area_sqkm = add_areasqkm(.))
      
     } else {
        
        logger::log_info("No rows left in 'xx2', continuing with only 'xx1'")
        
        out <-
          xx1 %>%  
          select(GNIS_ID, GNIS_NAME, COMID, FTYPE, member_comid) %>%
          mutate(area_sqkm = add_areasqkm(.))
        
        
      }
      
    # Write the resulting data to a new ".gpkg" file
    write_sf(out, outfile, "waterbodies")


  logger::log_info("Finished VPU ", which_VPU, " waterbodies")
  
  }
  
}

