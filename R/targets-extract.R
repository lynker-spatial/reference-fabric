rf.helper.get_conus_vpus <- function() {
  nhdplusTools::vpu_boundaries |>
    dplyr::arrange(VPUID) |>
    dplyr::filter(VPUID <= "18") |>
    sf::st_make_valid()
}

#' @export
rf.targets.get_topology <- function() {
  boundaries <- rf.helper.get_conus_vpus()

  sf::st_intersects(boundaries, sparse = FALSE) |>
    which(arr.ind = TRUE) |>
    apply(MARGIN = 1, \(x) sort(unname(x)), simplify = FALSE) |>
    unique() |>
    do.call(what = rbind, args = _) |>
    as.data.frame() |>
    stats::setNames(c("VPU1", "VPU2")) |>
    dplyr::filter(VPU1 != VPU2) |>
    dplyr::mutate(
      VPU1 = boundaries$VPUID[VPU1],
      VPU2 = boundaries$VPUID[VPU2]
    )
}

#' @export
rf.targets.get_enhd <- function(outfile) {
  if (!file.exists(outfile)) {
    sb_id <- "63cb311ed34e06fef14f40a3"
    sb_name <- "enhd_nhdplusatts.parquet"

    sbtools::item_file_download(
      sb_id,
      names = sb_name,
      destinations = outfile,
      overwrite_file = TRUE
    )
  }

  outfile
}

#' @export
rf.targets.get_vaa <- function(outfile) {
  if (!file.exists(outfile)) {
    tmp <- file.path(tempdir(TRUE), "vaa.parquet")
    on.exit(unlink(tmp))

    nhdplusTools::get_vaa(
      path = tmp,
      download = TRUE,
      updated_network = FALSE
    ) |>
      arrow::write_parquet(outfile)
  }

  outfile
}

#' @export
rf.targets.list_nhd <- function(dir.epa, dir.nhd) {
  epa_bucket <- "dmap-data-commons-ow"
  boundaries <- sf::st_drop_geometry(rf.helper.get_conus_vpus())

  urls <- 
    aws.s3::get_bucket_df(epa_bucket, prefix = "NHDPlusV21/Data/NHDPlus") |>
    dplyr::select(key = Key) |>
    dplyr::filter(endsWith(key, ".7z")) |>
    dplyr::mutate(file = basename(key)) |>
    dplyr::mutate(
      split = lapply(strsplit(file, "_", fixed = TRUE), utils::head, -1),
      region = vapply(split, dplyr::nth, character(1), 2),
      vpu = vapply(split, dplyr::nth, character(1), 3),
      type = vapply(split, dplyr::last, character(1))
    ) |>
    dplyr::select(key, region, vpu, type) |>
    dplyr::arrange(vpu, type) |>
    dplyr::filter(vpu %in% rf.helper.get_conus_vpus()$VPUID) |>
    dplyr::filter(type %in% c("NHDPlusCatchment", "NHDSnapshot", "NHDPlusBurnComponents")) |>
    dplyr::mutate(url = paste0("https://", epa_bucket, ".s3.amazonaws.com/", key)) |>
    dplyr::select(-key)
  
  # Format into type-specific data.frame
  .component <- function(.type, .shp, .fgb) {
    dplyr::filter(urls, type == !!.type) |>
      dplyr::mutate(
        archive = file.path(
          dir.epa,
          paste0(type, vpu, ".7z")
        ),
        shp = file.path(
          dir.epa,
          paste0("NHDPlus", region),
          paste0("NHDPlus", vpu),
          !!.type,
          !!.shp
        ),
        fgb = file.path(dir.nhd, !!.fgb, paste0("NHDPlus", vpu, ".fgb"))
      )
  }

  dplyr::bind_rows(
    .component("NHDPlusCatchment", "Catchment.shp", "catchments"),
    .component("NHDSnapshot", "Hydrography/NHDFlowline.shp", "flowlines"),
    .component("NHDSnapshot", "Hydrography/NHDWaterbody.shp", "waterbodies"),
    .component("NHDPlusBurnComponents", "BurnAddLine.shp", "burnlines")
  )
}

#' @export
rf.targets.download_usgs_poi <- function(outfile) {
  
  if (!file.exists(outfile)) {
    # using httr because SB has a looooong lag that times out with download.file
    sb_id   <- '61295190d34e40dd9c06bcd7'
    file_id <- 'reference_CONUS.gpkg'
    
    httr::GET(glue::glue('https://prod-is-usgs-sb-prod-publish.s3.amazonaws.com/{sb_id}/{file_id}'), 
              httr::write_disk(outfile, overwrite = TRUE), 
              httr::progress())
    
    n <- sf::st_layers(outfile)$name
    
    n <- n[!grepl("poi", n)]
    
    invisible(lapply(n, function(x) sf::st_delete(outfile, x)))
  }

  outfile
}

#' @export
rf.targets.download_nwm_poi <- function(outfile) {
  
  base <- 'https://spatial-water-noaa.s3.us-east-1.amazonaws.com'
  
  if (!file.exists(outfile)) {
   httr::GET(glue::glue('{base}/nwm/nwm_lakes.gpkg'), 
             httr::write_disk(outfile, overwrite = TRUE),
             httr::progress()
   )
  }
  
  outfile
}

#' @export
rf.targets.download_nws_poi <- function(outfile) {
  
  base <- 'https://spatial-water-noaa.s3.us-east-1.amazonaws.com'
  
  if (!file.exists(outfile)) {
    httr::GET(glue::glue('{base}/nwm/nws_lid.gpkg'), 
              httr::write_disk(outfile, overwrite = TRUE),
              httr::progress())
  }
  
  outfile
}

#' @export
rf.targets.download_coastal_poi <- function(outfile) {
  
  base <- 'https://spatial-water-noaa.s3.us-east-1.amazonaws.com'
  
  if (!file.exists(outfile)) {
    httr::GET(glue::glue('{base}/hydrofabric/pois/coastal_pois_twl.gpkg'), 
              httr::write_disk(outfile, overwrite = TRUE),
              httr::progress()
    )
  }
  
  outfile
}

#' @export
rf.targets.download_nhd <- function(.row) {
  .urls    <-  dplyr::select(.row, url, destfile = archive)
  url      <- .urls$url
  destfile <- .urls$destfile
  
  if (!file.exists(.row$shp)) {
    utils::download.file(url, destfile, mode = "wb")
    archive::archive_extract(destfile, dir = dirname(destfile))
    unlink(destfile)
  }
  
  .row$shp
}

#' @export
rf.targets.convert_nhd <- function(shp, .tbl) {
  fgb <- dplyr::filter(.tbl, shp == !!shp)$fgb

  if (length(fgb) == 0) {
    stop("Invalid shapefile path: ", shp)
  }

  rf.utils.ensure_directory(dirname(fgb))

  unlink(fgb)

  sf::gdal_utils(
    "vectortranslate",
    shp,
    fgb,
    options = c(
      "-f", "FlatGeobuf",
      "-makevalid",
      "-where", "\"_ogr_geometry_\" is not null",
      "-nlt", "PROMOTE_TO_MULTI",
      "-overwrite"
    )
  )

  fgb
}
