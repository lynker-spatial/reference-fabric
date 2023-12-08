#' Generate VPU Topology
#' @param vpus `sf` object containing VPU boundaries
#' @param output `character` path to output CSV
#' @note From `00_vpu_topo.R`
#' @return Path to output, i.e. `output`
rf_generate_vpu_topology <- function(vpus, output = "data/vpu_topology.csv") {
    dt <- sf::st_make_valid(vpus)

    x <- sf::st_intersects(dt, sparse = FALSE) |>
         which(arr.ind = TRUE) |>
         as.data.frame()

    vars <- lapply(seq_len(nrow(x)), function(y) {
        A <- as.numeric(x[y, ])
        A[order(A)]
    })

    do.call("rbind", vars)[!duplicated(vars), ] |>
        data.frame() |>
        setNames(c("VPU1", "VPU2")) |>
        dplyr::filter(VPU1 != VPU2) |>
        dplyr::mutate(VPU1 = dt$VPUID[VPU1],
                      VPU2 = dt$VPUID[VPU2]) |>
        write.csv(output, row.names = FALSE)

    output
}


#' Get VAA and handle failed attempt
#' @return result of nhdplusTools::get_vaa()
rf_get_vaa <- function() {
    tryCatch(
        nhdplusTools::get_vaa(),
        error = function(e) {
            unlink(nhdplusTools::get_vaa_path())
            nhdplusTools::get_vaa()
        }
    )
}

#' Drop ZM from `sf` object
#' @param obj `sf` Object
#' @return `obj` without ZM dimensions
rf_geometry_drop_zm <- function(obj) {
    sf::st_zm(obj)
}

#' Filter empty geometry out of an `sf` object
#' @param obj `sf` Object
#' @return `obj` without empty geometry rows
rf_filter_empty_geometry <- function(obj) {
    obj <- rf_geometry_drop_zm(obj)
    dplyr::filter(obj, !sf::st_is_empty(obj))
}

#' Remove substring from a string
#' @param x String to search through
#' @param pattern Regex passed to gsub
#' @param ... Additional arguments passed to gsub
#' @return `x` with pattern removed
rf_str_rm <- function(x, pattern, ...) {
    gsub(pattern = pattern, replacement = "", x, ...)
}

#' Extract VPU from NHDPlus GPKG path name
#' @param path Path containing VPU
#' @return vpuid
rf_extract_vpu <- function(path) {
    basename(path) |>
        rf_str_rm("NHDPlus") |>
        rf_str_rm(".gpkg")
}

rf_download_enhd <- function(outfile) {
    sb_id <- "63cb311ed34e06fef14f40a3"
    sbtools::item_file_download(
        sb_id,
        names = basename(outfile),
        destinations = outfile
    )
    outfile
}

.get_vpu <- function(x) {
    sapply(strsplit(x, "_"), `[`, 3)
}

.get_region <- function(x) {
    sapply(strsplit(x, "_"), `[`, 2)
}

.get_link <- function(bucket, x) {
    glue::glue("https://{bucket}.s3.amazonaws.com/{x}")
}

.make_key_df <- function(keys, type, outdir, bucket) {
    data.frame(
        key = keys,
        VPU = .get_vpu(keys),
        region = .get_region(keys),
        link = .get_link(bucket, keys)
    ) |>
        dplyr::mutate(
            outfile = file.path(
                outdir,
                glue::glue("NHDPlus{type}{VPU}.7z")
            )
        )
}

rf_epa_url_df <- function(
    epa_bucket_df,
    epa_bucket,
    epa_dir,
    cat_dir,
    fl_dir,
    wb_dir,
    ble_dir
) {
    cats <-
        grep("_NHDPlusCatchment_", epa_bucket_df$Key, value = TRUE) |>
        .make_key_df("Catchment", epa_dir, epa_bucket) |>
        dplyr::mutate(
            shp = file.path(
                epa_dir,
                glue::glue("NHDPlus{region}"),
                glue::glue("NHDPlus{VPU}"),
                "NHDPlusCatchment",
                "Catchment.shp"
            ),
            gpkg = file.path(
                cat_dir,
                glue::glue("NHDPlus{VPU}.gpkg")
            )
        )

    fl <-
        grep("_NHDSnapshot_", epa_bucket_df$Key, value = TRUE) |>
        .make_key_df("Snapshot", epa_dir, epa_bucket) |>
        dplyr::mutate(
            shp = file.path(
                epa_dir,
                glue::glue("NHDPlus{region}"),
                glue::glue("NHDPlus{VPU}"),
                "NHDSnapshot",
                "Hydrography",
                "NHDFlowline.shp"
            ),
            gpkg = file.path(
                fl_dir,
                glue::glue("NHDPlus{VPU}.gpkg")
            )
        )

    wb <-
        grep("_NHDSnapshot_", epa_bucket_df$Key, value = TRUE) |>
        .make_key_df("Snapshot", epa_dir, epa_bucket) |>
        dplyr::mutate(
            shp = file.path(
                epa_dir,
                glue::glue("NHDPlus{region}"),
                glue::glue("NHDPlus{VPU}"),
                "NHDSnapshot",
                "Hydrography",
                "NHDWaterbody.shp"
            ),
            gpkg = file.path(
                wb_dir,
                glue::glue("NHDPlus{VPU}.gpkg")
            )
        )

    ble <-
        grep("_NHDPlusBurnComponents_", epa_bucket_df$Key, value = TRUE) |>
        .make_key_df("Burn", epa_dir, epa_bucket) |>
        dplyr::mutate(
            shp = file.path(
                epa_dir,
                glue::glue("NHDPlus{region}"),
                glue::glue("NHDPlus{VPU}"),
                "NHDPlusBurnComponents",
                "BurnAddLine.shp"
            ),
            gpkg = file.path(
                ble_dir,
                glue::glue("NHDPlus{VPU}.gpkg")
            )
        )

    dplyr::bind_rows(cats, fl, wb, ble)
}

#' @param path Path to catchment GPKG
rf_clean_catchment <- function(catchment_path, flowpath_path, outfile, keep = 0.2) {
    catchment <- sf::read_sf(catchment_path)
    names(catchment) <- tolower(names(catchment))

    flowlines <- sf::read_sf(flowpath_path)
    names(flowlines) <- tolower(names(flowlines))

    out <- hydrofab::clean_geometry(
        catchments = catchment,
        flowlines  = flowlines,
        ID         = "featureid",
        fl_ID      = "comid",
        keep       = keep,
        sys        = TRUE
    )

    sf::write_sf(out, outfile, "catchments")

    outfile
}

rf_find_file_path = function(VPU, files, new_dir){
  f <- grep(VPU, basename(files))
  tmp01 <- files[f]
  tmp02 <- paste0(new_dir,  basename(files[f]))

  if (!file.exists(tmp02)){
    logger::log_info("\tCopying ", VPU, " to reference.")
    file.copy(grep(tmp01, files, value = TRUE), tmp02)
  }

  tmp02
}
