
# The purpose of this script is to process flowline data and improve network connectivity by using Burn Line Events flowlines

# Summary:
#   
#   1. Read and prepare the flowline data:
  # - Read the flowline data from a file path specified by `fl_path`.
  # - Perform necessary transformations: convert the data to an sf object, add Z and M dimensions (st_zm), and align the names (align_nhdplus_names).
  # - Join the flowline data with two other datasets (`vaa` and `new_atts`) based on the common identifier `COMID`.
  # - Select specific columns of interest from the joined data (COMID, fromnode, tonode, startflag, streamcalc, divergence, dnminorhyd).
  # - Perform another alignment of names.
  # - Compute a new column called LENGTHKM using the `add_lengthkm()` function.
# 
# 2. Filter and match COMIDs:
#   - Filter the `b` burn line events dataset to include only matching COMIDs found in the `nhd` data, creating the `ble` object.
#   - Find the matching indices of COMIDs between `ble` and `nhd`, storing the result in the `matcher` variable.
# 
# 3. Update geometry using matching indices:
#   - Replace the geometry in the `nhd` object with the geometry from `ble` at the matching indices, ensuring connectivity is corrected.
# 
# 4. Generate a custom network and update `override_tocomid`:
#   - Create a custom network (`custom_net`) based on the `nhd` data by selecting relevant columns (COMID, FromNode, ToNode, Divergence).
#   - Use the `get_tocomid()` function from the `nhdplusTools` package to generate a corrected `override_tocomid` column, considering node connections.
#   - Update the `nhd` data by performing a left join with `custom_net` based on matching COMIDs.
#   - Update the `override_tocomid` values in `nhd`, replacing them with values from the `toCOMID` column if `toCOMID` is not equal to 0. Otherwise, keep the original `override_tocomid` value.
# 
# CONCLUSION: read and processes flowline data
# --> aligns and joins relevant datasets
# --> corrects network connectivity issues by updating the geometry and `override_tocomid` values
# --> generates a custom network based on the updated data. 
# --> The goal is to ensure accurate representation and connectivity of river segments in the `nhd` object.

source('reference-features/nhdplusv2/config.R')

fl_paths  = list.files(fl_dir,  full.names = TRUE)
ble_paths = list.files(ble_dir, full.names = TRUE)
cat_paths = list.files(catchments_dir, full.names = TRUE)

new_atts = read_parquet(enhd)

tryCatch({
  
  vaa <- nhdplusTools::get_vaa()
  
}, error = function(e) {
  
  message("Error caught: ", conditionMessage(e))
  
  # Attempt to unlink the file
  unlink(nhdplusTools::get_vaa_path())
  
  # Retry getting VAA
  vaa <- nhdplusTools::get_vaa()
})


# BLE geopackage file path
output_file <- glue("{base_dir}/ble_events.gpkg")

# create Burn Line Events geopackage if it doesn't exist
if (!file.exists(output_file)) {
  
  message("Creating ble_events.gpkg...")
  
  # read in BurnLineEvents from NHD geodatabase
  ble <- 
    read_sf(nhdplus_ble_path, "BurnLineEvent") %>%
    st_zm() %>%
    filter(!st_is_empty(.))

  # getting starting and divergence flags from NHD VAAs
  nhdflag <- 
    vaa %>%
    filter(startflag == 1 | divergence == 2) %>%
    select(COMID = comid, vpuid)
  
  # join start/divergence flags with BLE by COMID
  ble <- 
    nhdflag %>% 
    left_join(
      ble, 
      by = "COMID"
      ) %>%
    st_as_sf() %>%
    filter(!st_is_empty(.))
  
  # select COMID and VPU from BLE and cast to LINESTRING
  ble <- 
    ble %>% 
    select( COMID, vpuid) %>%
    st_cast("LINESTRING")
  
  # save out BLE geopackage
  write_sf(ble, output_file)
  
  rm(ble)
  
}

# read in Burn Line Events geopackage
b <- read_sf(output_file)

# Loop through each file path in fl_paths
for(i in 1:length(fl_paths)){

  # Get the current file path
  fl_path   = fl_paths[i]
  
  # Extract the VPU identifier from the file path
  which_VPU = gsub(".gpkg", "", gsub("NHDPlus", "", basename(fl_path)))
  #ble_path  = ble_paths[grep(which_VPU, basename(ble_paths))]
  
  # Define the output file path for the processed flowlines
  outfile   = glue("{reference_dir}flowlines_{which_VPU}.gpkg")

  # Check if the output file does not exist
  if(!file.exists(outfile)){
    
    # Read flowlines from file, join with VAA and new_atts
    # 
    nhd <- 
      fl_path %>%
      read_sf() %>% 
      st_zm() %>%
      align_nhdplus_names() %>%
      left_join(
        vaa, 
        by = c("COMID" = "comid")
        ) %>%
      select(COMID, 
             fromnode, tonode, 
             startflag, streamcalc, 
             divergence, dnminorhyd) %>%
      left_join(
        new_atts, 
        by = c("COMID" = "comid")
        ) %>%
      align_nhdplus_names() %>%
      mutate(LENGTHKM  = add_lengthkm(.))
    
    cats = glue("{reference_dir}catchments_{which_VPU}.gpkg") %>% 
      read_sf() %>% 
      st_transform(5070)
    
    ### NEW ###
    
    ble_options <- 
      filter(nhd, COMID %in% b$COMID) %>% 
      st_transform(5070)
    
    st_geometry(ble_options) <- 
      st_geometry(get_node(ble_options, "start"))
    
    imap <- st_intersects(ble_options, cats)
  
    df <- data.frame(
      start = rep(ble_options$COMID, times = lengths(imap)),
      cat = cats$featureid[unlist(imap)]
    ) %>% 
      filter(start != cat)

    # Find the matching indices of COMIDs between 'ble' and 'nhd'
    ble = filter(nhd, COMID %in% df$start)
    matcher = match(df$start, nhd$COMID)

    # Replace the geometry in nhd geometry with ble geometries at the matching indices
    st_geometry(nhd)[matcher] <- st_geometry(ble)
    nhd$ble <- FALSE
    nhd$ble[matcher] <- TRUE
  
    # Generate a custom network based on 'nhd' data, and create a to override_tocomid column from get_tocomid() function
    custom_net <- 
      nhd %>% 
      st_drop_geometry() %>%
      select(COMID, FromNode, ToNode, Divergence) %>%
      nhdplusTools::get_tocomid(remove_coastal = FALSE) %>%
      select(comid, override_tocomid = tocomid)
    
    # left join nhd w/ custom_net on matching COMIDs, updating override_tocomid
    nhd <-
      nhd %>% 
      left_join(
        custom_net,
        by = c("COMID" = "comid")
        ) %>%
      # dplyr::relocate(COMID, toCOMID, override_tocomid)
      mutate(override_tocomid = ifelse(toCOMID == 0, override_tocomid, toCOMID))

    # is a headwater and for sure flows to something, where COMID is not in override_tocomid
    check <- !nhd$COMID %in% nhd$override_tocomid &
      !(nhd$override_tocomid == 0 | is.na(nhd$override_tocomid) |
          !nhd$override_tocomid %in% nhd$COMID)
    
    # filter nhd based on the check condition above
    check_direction <- filter(nhd, check)
    
    # Check not all of non 0 override_tocomid appear in nhd COMIDs, throw an error
    if(!all(check_direction$override_tocomid[check_direction$override_tocomid != 0] %in% nhd$COMID)){
      stop("this won't work")
    }
    
    # Find matching indices of override_tocomid in nhd and check_direction
    matcher <- match(check_direction$override_tocomid, nhd$COMID)
    
    # Get the matched rows from nhd
    matcher <- nhd[matcher, ]
    
    # Create a list combining check_direction and matcher
    fn_list <- Map(list,
                   split(check_direction, seq_len(nrow(check_direction))),
                   split(matcher, seq_len(nrow(matcher))))
    
    
    # make cluster of parallel workers
    cl <- makeCluster(par)

    # check and fix these, applies fix_flowdir() in parallel to each element of fn_list 
    new_geom <- pblapply(
      cl  = cl,
      X   = fn_list,
      FUN = function(x) {
        nhdplusTools::fix_flowdir(
                        x[[1]]$COMID,
                        fn_list = list(
                          flowline  = x[[1]],
                          network   = x[[2]],
                          check_end = "end"
                          )
                        )
      }
    )
    
    # stop parallel cluster     
    stopCluster(cl)
    
    # check for errors based on try-error conditions
    # identify the geometries that encountered errors during fix_flowdir() and store in "errors" 
    error_index <- sapply(new_geom, inherits, what = "try-error")
    
    # subset error geometries
    errors <- filter(nhd, COMID %in% check_direction$COMID[error_index])
    
    message("Number of errors: ", nrow(errors))
    
    message("check vector values:\n",
            paste0(names(table(check)), ": ", table(check), sep = "\n"))
    
    message("\nUpdating 'check' to account for errors in fix_flowdir()...\n")
    
    # update check as needed
    # marks the flowlines that do NOT meet the conditions for being a headwater, based on the errors that occurred.
    check[which(nhd$COMID %in% errors$COMID)] <- FALSE
    
    message("check vector values:\n",
      paste0(names(table(check)), ": ", table(check), sep = "\n")
    )

    # Check for errors other than empty geometry errors
    if(!all(sapply(st_geometry(errors), st_is_empty))) {
      stop("Errors other than empty geometry from fix flowdir")
    }
    
    message("Replacing NHD flowline geometries with directionaly correct flowline geometries...")
    
    # Replace the geometry in 'nhd' with the corrected geometries
    ng = do.call(c, new_geom[!error_index])
    switched = st_geometry(nhd)[check] != ng

    st_geometry(nhd)[check] <- ng
    nhd$reversed = switched
  
    # Filter nhd on COMIDs in new_atts and save out result to output file
    logger::log_info("Saving VPU ", which_VPU, " flowlines to:\n--> {outfile}")
    
    nhd |> 
      filter(COMID %in% new_atts$comid) |>
      select(-override_tocomid)  |> 
      mutate(lengthkm  = hydrofab::add_lengthkm(.)) |>
      st_cast("LINESTRING") |>
      write_sf(outfile, "flowlines")
  
  }

  logger::log_info("Finished VPU ", which_VPU, " flowlines")
}


## ---- test view the updated flow directions  ----
## RUN AFTER THE BELOW CODE has been run in the script above

# nhd2  <- nhd

# Replace the geometry in 'nhd' with the corrected geometries
# st_geometry(nhd2)[check] <- do.call(c, new_geom[!error_index])

# #  test view the updated flow directions 
# aoi <-
#   data.frame(lng = -81.30865, lat =  37.06332) %>%
#   st_as_sf(coords = c("lng", "lat"), crs = 4269) %>%
#   sf::st_transform(5070) %>%
#   sf::st_buffer(10000) %>%
#   sf::st_transform(4269)
# 
# # mapview::mapview(aoi)
# 
# orig_fline <- nhd[check,] %>%
#   sf::st_filter(aoi) %>%
#   dplyr::mutate(  source = "original")
# 
# new_fline <-  nhd2[check,] %>%
#   sf::st_filter(aoi) %>%
#   dplyr::mutate(source = "update" )
# # 
# flines <- dplyr::bind_rows(orig_fline, new_fline )
# # 
# end_pts <-  flines %>%
#   nhdplusTools::get_node(position = "end") %>%
#   dplyr::mutate( position = "end")
# 
# end_pts$COMID <- flines$COMID
# end_pts$source <- flines$source
# end_pts <- dplyr::relocate(end_pts, COMID, position, source, geometry)

# start_pts <-
#   flines %>% 
#   nhdplusTools::get_node(position = "start") %>% 
#   dplyr::mutate(
#     position = "start"
#   )
# 
# start_pts$COMID  <- flines$COMID
# start_pts$source <- flines$source
# start_pts <- dplyr::relocate(start_pts, COMID, position, source, geometry)
# 
# pts <- dplyr::bind_rows( end_pts, start_pts )
# 
# orig_fline <-  dplyr::filter(flines, source == "original")
# 
# orig_end <-  dplyr::filter(end_pts, source == "original", position == "end")
# 
# 
# new_fline <-  dplyr::filter(flines, source == "update")
# 
# new_end <-   dplyr::filter(end_pts, source == "update", position == "end")
# 
# mapview::mapview(orig_fline, color = "blue") + mapview::mapview(orig_end, col.regions = "blue") +
#   mapview::mapview(new_fline, color = "red") + mapview::mapview(new_end, col.regions = "red")
