source("targets-common.R", local = TRUE)

# =============================================================================
# Configuration ===============================================================
# =============================================================================
# nolint start
list(
    # Dir options
    dirs = list(
        tar_dir_path(config_dir_base, "output"),
        tar_dir_path(config_dir_data, config_dir_base, "00_data"),
        tar_dir_path(config_dir_epa, config_dir_base, "01_EPA_downloads"),
        tar_dir_path(config_dir_catchments, config_dir_base, "02_Catchments"),
        tar_dir_path(config_dir_flowlines, config_dir_base, "02_Flowlines"),
        tar_dir_path(config_dir_waterbodies, config_dir_base, "02_Waterbodies"),
        tar_dir_path(config_dir_ble, config_dir_base, "02_BLE"),
        tar_dir_path(config_dir_cleaned, config_dir_base, "03_cleaned_catchments"),
        tar_dir_path(config_dir_simplified, config_dir_base, "03_simplified_catchments"),
        tar_dir_path(config_dir_reference, config_dir_base, "04_reference_geometries"),
        tar_dir_path(config_dir_final, config_dir_base, "reference_features")
    ),

    # VPU options
    vpus = list(
        targets::tar_target(config_vpus_data, nhdplusTools::vpu_boundaries[1:21, ]),
        tar_file_path(config_vpus_output, config_dir_data, "vpu_topology.csv")
    ),

    # Other options
    targets::tar_target(config_epa_bucket, "dmap-data-commons-ow"),
    targets::tar_target(config_simplify_keep, .25),
    targets::tar_target(config_facfdr_crs, paste("+proj=aea", "+lat_0=23", "+lon_0=-96", "+lat_1=29.5", "+lat_2=45.5", "+x_0=0", "+y_0=0", "+datum=NAD83", "+units=m", "+no_defs")),
    tar_file_path(config_enhd, config_dir_base, "enhd_nhdplusatts.parquet"),
    tar_file_path(config_ble_gdb, config_dir_base, "NHDPlusNationalData", "NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
)
# nolint end
