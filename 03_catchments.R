source('workspace/reference-features/nhdplusv2/config.R')

files  = list.files(catchments_dir, full.names = TRUE, pattern = ".gpkg$")

flowpath_files = list.files(fl_dir,  full.names = TRUE, pattern = ".gpkg$")

out_gpkg    = glue("{simplified_dir}{gsub('NHDPlus', 'catchments_', basename(files))}")
out_geojson = glue("{simplified_dir}{gsub('NHDPlus', 'catchments_', basename(files))}")

# Simplify files -------------------------------------------------------------

for(i in 1:length(files)){

  if(!file.exists(out_gpkg[i])){
     # read in catchments
     catchments        = read_sf(files[i])
     # make names lowercase
     names(catchments) = tolower(names(catchments))
     # read in flowlines
     flowlines = read_sf(flowpath_files[i])
     names(flowlines) = tolower(names(flowlines))
     # clean geometries, dissolve internal bounds
     out = hydrofab::clean_geometry(
                         catchments = catchments,
                         flowlines  = flowlines,
                         ID         = "featureid",
                         fl_ID      = "comid",
                         keep       = num,
                         sys        = TRUE)
      write_sf(out, out_gpkg[i], "catchments")
   }
  
   message("Finished ", i, " of ", length(files))
}

# Clean borders ---------------------------------------------------------

tmpfile = glue('{reference_dir}/interm.geojson')
topos = read.csv(vpu_topo_csv)

for(i in 1:nrow(topos)){

  VPU1 = topos$VPU1[i]
  VPU2 = topos$VPU2[i]

  v_path_1 = find_file_path(VPU1, out_gpkg, cleaned_dir)
  v_path_2 = find_file_path(VPU2, out_gpkg, cleaned_dir)
  
  vpu_div_1 = read_sf(v_path_1, "catchments") %>% 
    mutate(vpuid = VPU1)
  
  vpu_div_2 = read_sf(v_path_2, "catchments") %>% 
    mutate(vpuid = VPU2)
  
  unlink(tmpfile)
  
  write_geojson_file(bind_rows(vpu_div_1, vpu_div_2), tmpfile)
  
  system(glue("mapshaper {tmpfile} -clean -o force {tmpfile}"))
  
  suppressWarnings({
    new  <- st_set_crs(read_geojson_file(tmpfile), st_crs(vpu_div_1)) %>% 
      select(featureid, vpuid) %>% 
      mutate(areasqkm = add_areasqkm(.))
  })
  
  write_sf(filter(new, vpuid == VPU1), v_path_1, "catchments", overwrite = TRUE)
  write_sf(filter(new, vpuid == VPU2), v_path_2, "catchments", overwrite = TRUE)

  log_info('Finished: ', i , " of ", nrow(topos))

}

unlink(tmpfile)

#######################################################################
cleaned_dir = glue('{dir}/clean/')

files = list.files(cleaned_dir, full.names = TRUE, pattern = "gpkg$") 

for(i in 1:length(files)) {
  cats = read_sf(files[i])
  
  imap = st_within(cats)
  
  df = data.frame(
    within    = rep(cats$featureid, times = lengths(imap)),
    featureid = cats$featureid[unlist(imap)]) |>
    filter(featureid != within)
  
  d <- list()
  u <- unique(df$featureid)
  
  if (length(u) > 0) {
    message("Islands found in: ", basename(files[i]))
    for (j in 1:length(u)) {
      d[[j]] <- st_difference(filter(cats, featureid %in% u[j]),
                             st_make_valid(st_combine(
                               filter(cats, featureid %in% filter(df, featureid == u[j])$within)
                             )),)
    }
    
    cats1 <- bind_rows(d)
    
    cats2 = filter(cats, !featureid %in% unlist(df)) %>%
      bind_rows(cats1)
    
    cats3 = filter(cats,
                   featureid %in% filter(df, !within %in% cats2$featureid)$within) %>%
      bind_rows(cats2) %>%
      st_cast("POLYGON") %>%
      mutate(areasqkm = add_areasqkm(.))
    
    log_info('\tWrite VPUS: ', basename(files[i]))
    
    stopifnot(nrow(cats3) == nrow(cats))
    
    write_sf(cats3, files[i], "catchments", overwrite = TRUE)
    
  }
}

####################################################################
