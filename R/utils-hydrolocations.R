# Summary:
# These fucntions provide utility functions for interacting with and processing
# National Water Model (NWM) NetCDF files. It includes functions to:
# - Retrieve the latest NWM version URL.
# - Find specific NWM files based on a given pattern.
# - Extract reservoir information into a structured table.
# - Convert NetCDF file variables into a tabular format.

#' @export
latest_nwm <- function() {
  ncep <- "https://www.nco.ncep.noaa.gov/pmb/codes/nwprod/"
  
  ver <- grep("nwm", readLines(ncep), value = TRUE)
  
  paste0(ncep, gsub("^.*href=\"\\s*|\\s*/.*$", "", ver))
}

#' @export
find_nwm_files <- function(url, 
                           pattern = "soilproperties|wrfinput|reservoir|Spatial_Metadata|RouteLink") {
  ver <- grep(pattern, readLines(url), value = TRUE)
  
  x <- strsplit(gsub('^.*href=\"\\s*|\\s*/.*$', "", ver[!grepl("Long", ver)]), ">") |> 
    lapply(`[[`, 1) |> 
    unlist()
  
  paste0(url, gsub('"', "", x))
}

#' @export
reservoir_to_table <- function(nc) {
  type <- gsub(".nc", "", basename(nc))
  
  nc <- RNetCDF::open.nc(nc)
  
  vars <- ncmeta::nc_vars(nc)$name
  
  lake <- dplyr::bind_cols(lapply(
    c("lake_id", "reservoir_type"),
    FUN = function(x) {
      RNetCDF::var.get.nc(nc, x)
    }
  ), .name_repair = "unique_quiet") |> 
    setNames(c("lake_id", "reservoir_type"))
  
  usgs <- if (any(grepl("usgs", vars))) {
    dplyr::bind_cols(lapply(
      c("usgs_lake_id", "usgs_gage_id"),
      FUN = function(x) {
        RNetCDF::var.get.nc(nc, x)
      }
    ), .name_repair = "unique_quiet") |> 
      setNames(c("lake_id", "gage_id")) |> 
      dplyr::mutate(owner = "usgs")
  } else {
    NULL
  }
  
  usace <- if (any(grepl("usace", vars))) {
    dplyr::bind_cols(lapply(
      c("usace_lake_id", "usace_gage_id"),
      FUN = function(x) {
        RNetCDF::var.get.nc(nc, x)
      }
    ), .name_repair = "unique_quiet") |> 
      setNames(c("lake_id", "gage_id")) |> 
      dplyr::mutate(owner = "usace")
  } else {
    NULL
  }
  
  rfc <- if (any(grepl("rfc", vars))) {
    dplyr::bind_cols(lapply(
      c("rfc_lake_id", "rfc_gage_id"),
      FUN = function(x) {
        RNetCDF::var.get.nc(nc, x)
      }
    ), .name_repair = "unique_quiet") |> 
      setNames(c("lake_id", "gage_id")) |> 
      dplyr::mutate(owner = "rfc")
  } else {
    NULL
  }
  
  dplyr::bind_rows(usgs, usace, rfc) |> 
    dplyr::right_join(lake, by = "lake_id") |> 
    dplyr::mutate(type = type) |> 
    dplyr::filter(complete.cases(.))
}

#' @export
nc_to_table <- function(nc) {
  nc <- RNetCDF::open.nc(nc)
  
  vars <- ncmeta::nc_vars(nc)$name
  
  lapply(
    seq_along(vars),
    FUN = function(x) {
      RNetCDF::var.get.nc(nc, vars[x])
    }
  ) |> 
    dplyr::bind_cols(.name_repair = "unique_quiet") |> 
    setNames(vars)
}
