# Setup
library(dplyr)
library(rmapshaper)
library(nhdplusTools)
library(hyRefactor)
library(sf)
library(units)
library(logger)
library(aws.s3)
library(archive)

sf_use_s2(FALSE)

epa_bucket = 'edap-ow-data-commons'
epa_download   = paste0(base_dir, '/01_EPA_downloads/')
catchments_dir = paste0(base_dir, '/02_Catchments/')
cleaned_dir    = paste0(base_dir, '/03_cleaned_catchments/')
simplified_dir = paste0(base_dir, '/04_simplified_catchments/')


dir.create(epa_download, showWarnings = FALSE)
dir.create(catchments_dir, showWarnings = FALSE)
dir.create(cleaned_dir, showWarnings = FALSE)
dir.create(simplified_dir, showWarnings = FALSE)
dir.create('data/', showWarnings = FALSE)

facfdr_crs = '+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'

num        = 20


pu_adjanceny = function(type = "vpu"){
  
  if(type == "vpu"){
    pu = nhdplusTools::vpu_boundaries
    id = "VPUID"
  } else {
    pu = nhdplusTools::rpu_boundaries
    id = "RPUID"
  }
  
  dt = sf::st_make_valid(pu)
  
  x = as.data.frame(which(st_intersects(dt, sparse = FALSE), arr.ind = T))
  
  vars = lapply(1:nrow(x), function(y){
    A <- as.numeric(x[y, ])
    A[order(A)]
  })
  
  do.call('rbind', vars)[!duplicated(vars),] |>
    data.frame() |>
    setNames(c("PU1", "PU2")) |>
    filter(PU1 != PU2) |>
    mutate(PU1 = dt[[id]][PU1],
           PU2 = dt[[id]][PU2]) 
}

find_file_path = function(VPU, files, new_dir){
  tmp01 = grep(VPU, files, value = TRUE)
  tmp02 = paste0(new_dir, "/", gsub("\\_.*", "", basename(tmp01)), ".gpkg")
  
  if(!file.exists(tmp02)){ file.copy(tmp01, tmp02) }
  
  tmp02
}
