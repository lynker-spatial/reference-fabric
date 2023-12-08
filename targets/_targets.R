source("targets-common.R", local = TRUE)

sf::sf_use_s2(FALSE)

targets::tar_option_set(
    packages = c(
        "hydrofab", "nhdplusTools", "glue", "logger", "archive",
        "rmapshaper", "aws.s3", "sbtools", "sfheaders", "arrow",
        "nngeo", "vctrs", "foreach"
    ),
    memory = "transient",
    garbage_collection = TRUE,
    controller = crew::crew_controller_local(
        workers         = 3L,
        seconds_idle    = 10L,
        launch_max      = Inf,
        seconds_timeout = 30,
        seconds_launch  = 60L,
        garbage_collection = TRUE
    )
)

#' Load targets from a file
#' @note expects that the file `path` has a list as the last object.
load_targets_from_file <- function(path) {
    source(path, local = TRUE)$value
}

.targets             <- new.env()
.targets$config      <- load_targets_from_file("targets-config.R")
.targets$nhdplus     <- load_targets_from_file("targets-nhdplus.R")
.targets$ble         <- load_targets_from_file("targets-ble.R")
.targets$waterbodies <- load_targets_from_file("targets-waterbodies.R")
.targets$flowlines   <- load_targets_from_file("targets-flowlines.R")
.targets$catchments  <- load_targets_from_file("targets-catchments.R")
.targets$merge       <- load_targets_from_file("targets-merge.R")

# =============================================================================
# VPU Topology Targets ========================================================
# =============================================================================

.targets$vpu_topo <- list(
    tar_target(
        rf_vpus_topology_file,
        rf_generate_vpu_topology(
            vpus = config_vpus_data,
            output = config_vpus_output
        ),
        format = "file"
    )
)


# =============================================================================
# Pipeline ====================================================================
# =============================================================================
list(
    .targets$config,
    # 00_vpu_topo
    .targets$vpu_topo,
    # 01_nhdplus_data
    .targets$nhdplus,
    # 02_waterbodies
    .targets$waterbodies,
    # 03_flowlines
    .targets$ble,
    .targets$flowlines,
    # 04_catchments
    .targets$catchments,
    .targets$merge
)
