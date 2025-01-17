#' @keywords internal
rf.internal.options <- function() {
  config <- new.env()

  base::with(config, {
    rf.config.dir.base       <- "reference.fabric"
    rf.config.dir.data       <- file.path(rf.config.dir.base, "00_data")
    rf.config.dir.epa        <- file.path(rf.config.dir.base, "01_epa")
    rf.config.dir.nhd        <- file.path(rf.config.dir.base, "02_nhd")
    rf.config.dir.cleaned    <- file.path(rf.config.dir.base, "03_clean")
    rf.config.dir.simplified <- file.path(rf.config.dir.base, "04_simplified")
    rf.config.dir.reference  <- file.path(rf.config.dir.base, "05_reference")
    rf.config.dir.output     <- file.path(rf.config.dir.base, "06_output")
    rf.config.file.enhd      <- file.path(rf.config.dir.data, "enhd_nhdplusatts.parquet")
    rf.config.file.vaa       <- file.path(rf.config.dir.data, "vaa_nhdplusatts.parquet")
    rf.config.file.nhdplus   <- file.path(rf.config.dir.data, "NHDPlusNationalData", "NHDPlusV21_National_Seamless_Flattened_Lower48.gdb")
    rf.config.file.usgs_poi   <- file.path(rf.config.dir.data, "usgs_poi_file.gpkg")
    rf.config.epa_bucket     <- "dmap-data-commons-ow"
    rf.config.simplify_keep  <- 0.20
    rf.config.facfdr_crs     <- "+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs"
  })

  as.list(config)
}
