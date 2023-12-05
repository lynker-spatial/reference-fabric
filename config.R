library("hydrofab")
library("nhdplusTools")
library("glue")
library("logger")
library("archive")
library("rmapshaper")
library("aws.s3")
library("sbtools")
library("sfheaders")
library("parallel")
library("pbapply")
library("arrow")
library("nngeo")

sf_use_s2(FALSE)

source("reference-features/nhdplusv2/root_dir.R")

vpus =  vpu_boundaries[1:21,]
n    =  'enhd_nhdplusatts.parquet'
enhd =  glue("{base_dir}/{n}")

vpu_topo_csv = "data/vpu_topology.csv"

facfdr_crs = '+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'

num        = .20

par = 3

epa_bucket     = 'dmap-data-commons-ow'

# path to NHDPlus BLE events folder
nhdplus_ble_path <- glue("{base_dir}/NHDPlusNationalData/NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")

epa_download   = glue('{base_dir}/01_EPA_downloads/')
  dir.create(epa_download,   showWarnings = FALSE)
catchments_dir = glue('{base_dir}/02_Catchments/')
  dir.create(catchments_dir, showWarnings = FALSE, recursive = TRUE)
fl_dir         = glue('{base_dir}/02_Flowlines/')
  dir.create(fl_dir,         showWarnings = FALSE, recursive = TRUE)
wb_dir         = glue('{base_dir}/02_Waterbodies/')
  dir.create(wb_dir,         showWarnings = FALSE, recursive = TRUE)
ble_dir        = glue('{base_dir}/02_BLE/')
  dir.create(ble_dir,        showWarnings = FALSE, recursive = TRUE)
cleaned_dir    = glue('{base_dir}/03_cleaned_catchments/')
  dir.create(cleaned_dir,    showWarnings = FALSE, recursive = TRUE)
simplified_dir = glue('{base_dir}/03_simplified_catchments/')
  dir.create(simplified_dir, showWarnings = FALSE, recursive = TRUE)
reference_dir  = glue('{base_dir}/04_reference_geometries/')
  dir.create(reference_dir,  showWarnings = FALSE, recursive = TRUE)
final_dir  = glue('{base_dir}/reference_features/')
  dir.create(final_dir,      showWarnings = FALSE, recursive = TRUE)



find_file_path = function(VPU, files, new_dir){
  f = grep(VPU, basename(files))
  tmp01 = files[f]
  tmp02 = paste0(new_dir,  basename(files[f]))
  
  if(!file.exists(tmp02)){
    log_info('\tCopying ', VPU, " to reference.")
    file.copy(grep(tmp01, files, value = TRUE), tmp02)
  }
  
  tmp02
}
  
  





