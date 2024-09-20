library(targets)

# Pipeline Configuration Options ----------------------------------------------
rf.config.dir.base          <- getOption("rf.config.dir.base", "reference-fabric")
rf.config.dir.data          <- file.path(rf.config.dir.base, "00_data")
rf.config.dir.epa           <- file.path(rf.config.dir.base, "01_epa")
rf.config.dir.nhd           <- file.path(rf.config.dir.base, "02_nhd")
rf.config.dir.cleaned       <- file.path(rf.config.dir.base, "03_clean")
rf.config.dir.simplified    <- file.path(rf.config.dir.base, "04_simplified")
rf.config.dir.reference     <- file.path(rf.config.dir.base, "05_reference")
rf.config.dir.output        <- file.path(rf.config.dir.base, "06_output")
rf.config.file.enhd         <- getOption("rf.config.file.enhd", file.path(rf.config.dir.data, "enhd_nhdplusatts.parquet"))
rf.config.file.vaa          <- getOption("rf.config.file.vaa", file.path(rf.config.dir.data, "vaa_nhdplusatts.parquet"))
rf.config.file.nhdplus      <- getOption("rf.config.file.nhdplus", file.path(rf.config.dir.data, "NHDPlusNationalData", "NHDPlusV21_National_Seamless_Flattened_Lower48.gdb"))
rf.config.epa_bucket        <- getOption("rf.config.epa_bucket", "dmap-data-commons-ow")
rf.config.simplify_keep     <- getOption("rf.config.simplify_keep", 0.20)
rf.config.facfdr_crs        <- getOption("rf.config.facfdr_crs", "+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs")
# -----------------------------------------------------------------------------

list(
  ## =============== Extract =============== ##

  # VPU Boundary Topology
  targets::tar_target(rf_vpu_topology,
    reference.fabric::rf.targets.get_topology()
  ),

  # E2NHDPlusV2 River Network Attributes
  targets::tar_target(rf_enhd_file,
    reference.fabric::rf.targets.get_enhd(rf.config.file.enhd),
    format = "file"
  ),
  targets::tar_target(rf_enhd,
    arrow::read_parquet(rf_enhd_file)
  ),

  # NHDPlusV2 Value-added Attributes
  targets::tar_target(rf_vaa_file,
    reference.fabric::rf.targets.get_vaa(rf.config.file.vaa),
    format = "file"
  ),
  targets::tar_target(rf_vaa,
    arrow::read_parquet(rf_vaa_file)
  ),

  # NHDPlusV2
  targets::tar_target(rf_list_nhd,
    reference.fabric::rf.targets.list_nhd(rf.config.dir.epa, rf.config.dir.nhd),
    iteration = "vector"
  ),
  targets::tar_target(rf_download_nhd,
    reference.fabric::rf.targets.download_nhd(rf_list_nhd),
    pattern = map(rf_list_nhd),
    format = "file"
  ),
  targets::tar_target(rf_convert_nhd,
    reference.fabric::rf.targets.convert_nhd(rf_download_nhd, rf_list_nhd),
    pattern = map(rf_download_nhd),
    format = "file"
  ),
  targets::tar_target(rf_nhd_catchment_path,
    sort(grep("catchments", rf_convert_nhd, value = TRUE))
  ),
  targets::tar_target(rf_nhd_flowline_path,
    sort(grep("flowlines", rf_convert_nhd, value = TRUE))
  ),
  targets::tar_target(rf_nhd_waterbody_path,
    sort(grep("waterbodies", rf_convert_nhd, value = TRUE))
  ),
  targets::tar_target(rf_nhd_burnline_path,
    sort(grep("burnlines", rf_convert_nhd, value = TRUE))
  )

  ## =============== Processing Waterbodies =============== ##

  ## =============== Processing Flowlines =============== ##

  ## =============== Processing Catchments =============== ##

  ## =============== Output Reference Features =============== ##
)
