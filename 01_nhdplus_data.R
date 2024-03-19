source('reference-features/nhdplusv2/config.R')

epa = get_bucket_df(epa_bucket, max = Inf)

#### New Attributes ------

item_file_download(sb_id = '63cb311ed34e06fef14f40a3', names = n, destinations = enhd, overwrite_file = TRUE)

# Catchments --------------------------------------------------------------

#### Define
cats = grep("_NHDPlusCatchment_", epa$Key, value = TRUE)

all_cats = data.frame(
  key    = cats,
  VPU    = sapply(strsplit(cats, "_"), `[`, 3),
  region = sapply(strsplit(cats, "_"), `[`, 2),
  link   = glue('https://{epa_bucket}.s3.amazonaws.com/{cats}')) %>%
  mutate(
    outfile = glue('{epa_download}/NHDPlus{VPU}.7z'),
    shp = glue("{epa_download}NHDPlus{region}/NHDPlus{VPU}/NHDPlusCatchment/Catchment.shp")) %>%
  filter(!shp %in% list.files(epa_download, recursive  = TRUE, pattern = ".shp$"))

#### Download & Extract

for (i in 1:nrow(all_cats)) {
  save_object(object = all_cats$key[i], bucket = epa_bucket, file = all_cats$outfile[i])

  archive_extract(all_cats$outfile[i], dir = epa_download)

  unlink(all_cats$outfile[i])
  log_info("Downloaded and extracted ", all_cats$VPU[i])
}

#### To geopackage

all_cats = all_cats %>%
  mutate(gpkg = glue('{catchments_dir}NHDPlus{all_cats$VPU}.gpkg')) %>%
  filter(!gpkg %in% list.files(catchments_dir, full.names = TRUE))

if(nrow(all_cats) > 0){
  calls = paste('ogr2ogr -f GPKG -nlt MULTIPOLYGON', all_cats$gpkg, all_cats$shp)

  for(i in 1:length(calls)){
    system(calls[i])
    log_info("Produced ", basename(all_cats$gpkg[i]))
  }
}

# Flowlines + Waterbodies --------------------------------------------------------------

snap = grep("_NHDSnapshot_", epa$Key, value =TRUE)

all_snap = data.frame(
  key = snap,
  VPU = sapply(strsplit(snap, "_"), `[`, 3),
  region = sapply(strsplit(snap, "_"), `[`, 2),
  link = glue('https://{epa_bucket}.s3.amazonaws.com/{snap}')) %>%
  mutate(outfile = glue("{epa_download}NHDPlusSnapshot{VPU}.7z"),
         fl_shp  = glue("{epa_download}NHDPlus{region}/NHDPlus{VPU}/NHDSnapshot/Hydrography/NHDFlowline.shp"),
         wb_shp  = glue("{epa_download}NHDPlus{region}/NHDPlus{VPU}/NHDSnapshot/Hydrography/NHDWaterbody.shp")) %>%
  filter(!fl_shp %in% list.files(epa_download, recursive  = TRUE, pattern = ".shp$"))

####

for(i in 1:nrow(all_snap)){
  save_object(object = all_snap$key[i], bucket = epa_bucket, file = all_snap$outfile[i])
  archive_extract(all_snap$outfile[i], dir = epa_download)
  unlink(all_snap$outfile[i])
  log_info("Downloaded and extracted ", all_snap$VPU[i])
}

####

all_snap <- 
  all_snap %>%
  mutate(fl_gpkg = glue("{fl_dir}NHDPlus{VPU}.gpkg"),
         wb_gpkg = glue("{wb_dir}NHDPlus{VPU}.gpkg")) %>%
  filter(!wb_gpkg %in% list.files(wb_dir, full.names = TRUE))

if(nrow(all_snap) > 0){
  calls = c(
    paste('ogr2ogr -f GPKG -nlt MULTIPOLYGON', all_snap$wb_gpkg, all_snap$wb_shp),
    paste('ogr2ogr -f GPKG -nlt MULTILINESTRING', all_snap$fl_gpkg, all_snap$fl_shp)
  )

  for(i in 1:length(calls)){
    system(calls[i])
    message(i)
  }
}

# BLE --------------------------------------------------------------

burn = grep("_NHDPlusBurnComponents_", epa$Key, value =TRUE)

all_burn = data.frame(
  key = burn,
  VPU = sapply(strsplit(burn, "_"), `[`, 3),
  region = sapply(strsplit(burn, "_"), `[`, 2),
  link = glue('https://{epa_bucket}.s3.amazonaws.com/{burn}')) |>
  mutate(outfile = glue("{epa_download}NHDPlusBurnComponents{VPU}.7z"),
         ble_shp  = glue("{epa_download}NHDPlus{region}/NHDPlus{VPU}/NHDPlusBurnComponents/BurnAddLine.shp")) %>%
  filter(!ble_shp %in% list.files(epa_download, recursive  = TRUE, pattern = ".shp$"))

for(i in 1:nrow(all_burn)){
  save_object(object = all_burn$key[i], bucket = epa_bucket, file = all_burn$outfile[i])
  archive_extract(all_burn$outfile[i], dir = epa_download)
  unlink(all_burn$outfile[i])
  log_info("Downloaded and extracted ", all_burn$VPU[i])
}

all_burn = all_burn %>%
  mutate(ble_gpkg = paste0(ble_dir, "/NHDPlus", VPU, ".gpkg")) %>%
  filter(!ble_gpkg %in% list.files(ble_dir, full.names = TRUE))

if(nrow(all_burn) > 0){
  calls = paste('ogr2ogr -f GPKG -nlt MULTILINESTRING', all_burn$ble_gpkg, all_burn$ble_shp)

  for(i in 1:length(calls)){
    system(calls[i])
    message(i)
  }
}


# CatchmentSP --------------------------------------------------------------

sp = grep("NHDPlusCatchment2", epa$Key, value =TRUE)

all_sp = data.frame(
  key = sp,
  VPU = sapply(strsplit(sp, "_"), `[`, 3),
  region = sapply(strsplit(sp, "_"), `[`, 2),
  version = sapply(strsplit(sp, "_"), `[`, 5),
  link = glue('https://{epa_bucket}.s3.amazonaws.com/{sp}')) |>
  mutate(outfile = glue("{epa_download}NHDPlusCatchment2{VPU}.7z"),
         sp_shp  = glue("{epa_download}NHDPlus{region}/NHDPlus{VPU}/NHDPlusCatchment/CatchmentSP.shp")) %>%
  filter(!sp_shp %in% list.files(epa_download, recursive  = TRUE, pattern = ".shp$"))

for(i in 1:nrow(all_sp)){
  save_object(object = all_sp$key[i], bucket = epa_bucket, file = all_sp$outfile[i])
  archive_extract(all_sp$outfile[i], dir = epa_download)
  unlink(all_sp$outfile[i])
  log_info("Downloaded and extracted ", all_sp$VPU[i])
}

all_sp = all_sp %>%
  mutate(sp_gpkg = paste0(sp_dir, "/NHDPlus", VPU, ".gpkg")) %>%
  filter(!sp_gpkg %in% list.files(ble_dir, full.names = TRUE))

if(nrow(all_sp) > 0){
  calls = paste('ogr2ogr -f GPKG -nlt MULTILINESTRING', all_sp$sp_gpkg, all_sp$sp_shp)
  
  for(i in 1:length(calls)){
    system(calls[i])
    message(i)
  }
}
