#' @export
rf.targets.merge_info <- function(catchments, flowlines, waterbodies, output_directory) {

  catchments  <- normalizePath(catchments)
  flowlines   <- normalizePath(flowlines)
  waterbodies <- normalizePath(waterbodies)

  data.frame(path = c(catchments, flowlines, waterbodies)) |>
    dplyr::mutate(
      vpu = reference.fabric::rf.utils.extract_vpu(path),
      layer = tools::file_path_sans_ext(basename(path)),
      outfile = file.path(output_directory, paste0(vpu, "_reference_features.gpkg"))
    ) |>
    dplyr::arrange(vpu) |>
    dplyr::group_by(vpu)
}

#' @export
rf.targets.merge_vpu <- function(merge_info) {
  split(merge_info, seq_len(nrow(merge_info))) |>
    lapply(function(info) {
      rf.utils.ensure_directory(dirname(info$outfile))
      sf::gdal_utils(
        "vectortranslate",
        source = info$path,
        destination = info$outfile,
        options = c(
          "-f", "GPKG",
          "-t_srs", "EPSG:5070",
          "-lco", paste0("FID=", ifelse(info$layer == "catchments", "featureid", "COMID")),
          "-sql", paste0(
            "SELECT * FROM ",
            info$layer,
            " WHERE \"_ogr_geometry_\" is not null ORDER BY ",
            ifelse(info$layer == "catchments", "featureid", "COMID"),
            " DESC"
          ),
          "-makevalid",
          "-upsert",
          "-nln", info$layer
        )
      )
    })
  
  unique(merge_info$outfile)
}

#' @export
rf.targets.merge_conus <- function(merge_info) {
  outfile <-
    merge_info$outfile |>
    unique() |>
    dirname() |>
    file.path("conus_reference_features.gpkg")

  .tbl <-
    dplyr::bind_rows(merge_info) |>
    dplyr::ungroup()

  layers <- unique(.tbl$layer)
  for (layer in layers) {
    dplyr::filter(.tbl, layer == !!layer) |>
      dplyr::pull(path) |>
      lapply(function(.path) {
        sf::gdal_utils(
          "vectortranslate",
          source = .path,
          destination = outfile,
          options = c(
            "-f", "GPKG",
            "-t_srs", "EPSG:5070",
            "-lco", paste0("FID=", ifelse(layer == "catchments", "featureid", "COMID")),
            "-sql", paste0(
              "SELECT * FROM ",
              layer,
              " WHERE \"_ogr_geometry_\" is not null ORDER BY ",
              ifelse(layer == "catchments", "featureid", "COMID"),
              " DESC"
            ),
            "-makevalid",
            "-upsert",
            "-nln", layer
          )
        )
      })
  }

  outfile
}
