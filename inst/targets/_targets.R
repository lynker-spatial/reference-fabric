library(targets)

# so parallel workers do I/O with the store
tar_option_set(storage = "worker", retrieval = "worker")

library(future)
library(future.callr)
plan(multisession)

# Pipeline Configuration Options ----------------------------------------------
rf.config.dir.base       <- getOption("rf.config.dir.base", "reference_fabric")
rf.config.dir.data       <- file.path(rf.config.dir.base, "00_data")
rf.config.dir.epa        <- file.path(rf.config.dir.base, "01_epa")
rf.config.dir.nhd        <- file.path(rf.config.dir.base, "02_nhd")
rf.config.dir.cleaned    <- file.path(rf.config.dir.base, "03_clean")
rf.config.dir.simplified <- file.path(rf.config.dir.base, "04_simplified")
rf.config.dir.reference  <- file.path(rf.config.dir.base, "05_reference")
rf.config.dir.output     <- file.path(rf.config.dir.base, "06_output")
rf.config.file.enhd      <- getOption("rf.config.file.enhd", file.path(rf.config.dir.data, "enhd_nhdplusatts.parquet"))
rf.config.file.vaa       <- getOption("rf.config.file.vaa", file.path(rf.config.dir.data, "vaa_nhdplusatts.parquet"))
rf.config.file.nhdplus   <- getOption("rf.config.file.nhdplus", file.path(rf.config.dir.data, "NHDPlusNationalData", "NHDPlusV21_National_Seamless_Flattened_Lower48.gdb"))
rf.config.epa_bucket     <- getOption("rf.config.epa_bucket", "dmap-data-commons-ow")
rf.config.simplify_keep  <- getOption("rf.config.simplify_keep", 0.20)
rf.config.facfdr_crs     <- getOption("rf.config.facfdr_crs", "+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs")
# -----------------------------------------------------------------------------

reference.fabric::rf.utils.ensure_directory(rf.config.dir.data)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.epa)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.nhd)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.cleaned)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.simplified)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.reference)
reference.fabric::rf.utils.ensure_directory(rf.config.dir.output)

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
    format = "file", deployment = "worker"
  ),
  targets::tar_target(rf_convert_nhd,
    reference.fabric::rf.targets.convert_nhd(rf_download_nhd, rf_list_nhd),
    pattern = map(rf_download_nhd),
    format = "file", deployment = "worker"
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
  ),

  ## =============== Processing Waterbodies =============== ##
  targets::tar_target(rf_wb_clean_waterbodies,
    reference.fabric::rf.targets.clean_waterbodies(
      rf_nhd_waterbody_path,
      rf.config.dir.cleaned
    ),
    pattern = map(rf_nhd_waterbody_path),
    format = "file", deployment = "worker"
  ),
  targets::tar_target(rf_wb_reference_waterbodies,
    reference.fabric::rf.targets.reference_waterbodies(
      rf_wb_clean_waterbodies,
      reference.fabric::rf.utils.extract_vpu(rf_wb_clean_waterbodies),
      rf.config.dir.reference
    ),
    pattern = map(rf_wb_clean_waterbodies),
    format = "file", deployment = "worker"
  ),

  ## =============== Processing Burnlines =============== ##
  targets::tar_target(rf_burnline_events,
    reference.fabric::rf.targets.burnline_events(
      nhd_gdb = rf.config.file.nhdplus,
      rf_vaa = rf_vaa,
      dir_data = rf.config.dir.data
    ),
    format = "file"
  ),

  ## =============== Processing Catchments =============== ##
  targets::tar_target(rf_cat_info,
    targets::tar_group(
      reference.fabric::rf.targets.catchment_info(
        rf_vpu_topology,
        rf_nhd_catchment_path,
        rf_nhd_flowline_path,
        rf.config.dir.cleaned
      )
    ),
    iteration = "group"
  ),
  targets::tar_target(rf_cat_clean_catchments,
    reference.fabric::rf.targets.clean_catchment(rf_cat_info, rf.config.simplify_keep),
    pattern = map(rf_cat_info),
    format = "file"
  ),
  targets::tar_target(rf_cat_rectify_borders,
    reference.fabric::rf.targets.rectify_catchment_borders(
      rf_cat_clean_catchments,
      rf_vpu_topology,
      rf.config.dir.reference
    )
  ),
  targets::tar_target(rf_cat_reference_catchments,
    reference.fabric::rf.targets.reference_catchments(rf_cat_rectify_borders),
    pattern = map(rf_cat_rectify_borders),
    format = "file", deployment = "worker"
  ),

  ## =============== Processing Flowlines =============== ##
  targets::tar_target(rf_fl_clean_flowlines,
    reference.fabric::rf.targets.clean_flowlines(
      rf_nhd_flowline_path,
      rf_cat_reference_catchments,
      rf_burnline_events,
      rf_vaa,
      rf_enhd,
      rf.config.dir.cleaned
    ),
    pattern = map(rf_nhd_flowline_path),
    format = "file", deployment = "worker"
  ),
  targets::tar_target(rf_fl_reference_flowlines,
    reference.fabric::rf.targets.reference_flowlines(
      rf_fl_clean_flowlines,
      reference.fabric::rf.utils.extract_vpu(rf_fl_clean_flowlines),
      rf_enhd$comid,
      rf.config.dir.reference
    ),
    pattern = map(rf_fl_clean_flowlines),
    format = "file", deployment = "worker"
  ),

  ## =============== Output Reference Features =============== ##
  targets::tar_target(rf_merge_info,
    targets::tar_group(
      reference.fabric::rf.targets.merge_info(
        catchments = rf_cat_reference_catchments,
        flowlines  = rf_fl_reference_flowlines,
        waterbodies = rf_wb_reference_waterbodies,
        output_directory = rf.config.dir.output
      )
    ),
    iteration = "group"
  ),
  targets::tar_target(rf_merge_vpu,
    reference.fabric::rf.targets.merge_vpu(rf_merge_info),
    pattern = map(rf_merge_info),
    format = "file", deployment = "worker"
  ),
  targets::tar_target(rf_merge_conus,
    reference.fabric::rf.targets.merge_conus(rf_merge_info),
    format = "file"
  )
)
